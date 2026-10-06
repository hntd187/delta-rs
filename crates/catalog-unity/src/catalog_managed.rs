use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::UnityCatalog;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use delta_kernel::LogPath;
use deltalake_core::kernel::Version;
use deltalake_core::kernel::transaction::TransactionError;
use deltalake_core::logstore::object_store::{ObjectStore, ObjectStoreExt};
use deltalake_core::logstore::{
    CatalogLogTail, CommitOrBytes, CommitResponse, Committer, LogStore, LogStoreConfig,
    ObjectStoreRef, PayloadKind, StorageConfig,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use unity_catalog_delta_client_api::{
    Commit, DeltaTableRequirement, DeltaTableUpdate, Operation, TableIdentifier,
    UpdateTableClient, UpdateTableRequest,
};
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct CommitList {
    pub commits: Vec<Commit>,
    pub max_version: Version,
}

#[async_trait]
pub trait CommitCoordinator: Send + Sync + std::fmt::Debug {
    async fn get_commits(&self) -> DeltaResult<CommitList>;
}

#[derive(Debug)]
pub struct UnityCommitCoordinator {
    client: UnityCatalog,
    catalog: String,
    schema: String,
    table: String,
    config: StorageConfig,
    staged: Mutex<HashMap<Version, (ObjectStoreRef, Path)>>,
}

impl UnityCommitCoordinator {
    pub fn new(
        client: UnityCatalog,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        config: StorageConfig,
    ) -> Self {
        Self {
            client,
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
            config,
            staged: Mutex::new(HashMap::new()),
        }
    }
}

/// The staged-commit `file_name` and object-store `Path` for `version` under
/// `_delta_log/_staged_commits/`, using a random UUID to keep concurrent writers' files distinct.
fn staged_commit_file(version: Version, uuid: Uuid) -> (String, Path) {
    let file_name = format!("{version:020}.{uuid}.json");
    let path = Path::from(format!("_delta_log/_staged_commits/{file_name}"));
    (file_name, path)
}

/// Wrap an error as a `TransactionError` the commit loop treats as a hard (non-conflict) failure.
fn commit_failed(msg: String) -> TransactionError {
    TransactionError::LogStoreError {
        source: Box::new(DeltaTableError::Generic(msg.clone())),
        msg,
    }
}

/// Whether a UC `update_table` error means "this version was already ratified".
///
/// TODO(live): confirm the exact status / UC `ErrorCode` for a version conflict against a real
/// workspace. The `UpdateTableClient` trait erases the HTTP status into
/// `Error::Generic("HTTP error (status <code>): <body>")`, so provisionally we match a 409 in
/// that message (write plan §5).
fn is_version_conflict(err: &unity_catalog_delta_client_api::Error) -> bool {
    matches!(
        err,
        unity_catalog_delta_client_api::Error::Generic(msg) if msg.contains("status 409")
    )
}

#[async_trait]
impl Committer for UnityCommitCoordinator {
    async fn commit(
        &self,
        version: Version,
        payload: CommitOrBytes,
    ) -> Result<CommitResponse, TransactionError> {
        let bytes = match payload {
            CommitOrBytes::LogBytes(bytes) => bytes,
            CommitOrBytes::TmpCommit(_) => {
                return Err(commit_failed(
                    "CatalogManagedLogStore requires a LogBytes payload".to_string(),
                ));
            }
        };

        let client = self
            .client
            .delta_rest_client()
            .map_err(|e| commit_failed(format!("UC client init failed: {e}")))?;
        let loaded = client
            .load_table(&self.catalog, &self.schema, &self.table)
            .await
            .map_err(|e| commit_failed(format!("UC load_table failed: {e}")))?;
        let table_uuid = loaded.metadata.table_uuid;
        let location = loaded.metadata.location;

        let creds = client
            .get_table_credentials(&self.catalog, &self.schema, &self.table, Operation::ReadWrite)
            .await
            .map_err(|e| commit_failed(format!("UC READ_WRITE credential vending failed: {e}")))?;
        let cred_options = crate::storage_credentials_to_options(&creds.storage_credentials);
        let store = crate::build_object_store(&location, cred_options, &self.config)
            .map_err(|e| commit_failed(format!("failed to build write store: {e}")))?;

        // Write the staged commit file: _delta_log/_staged_commits/<version:020>.<uuid>.json
        let (file_name, staged_path) = staged_commit_file(version, Uuid::new_v4());
        let file_size = bytes.len() as i64;
        self.staged
            .lock()
            .unwrap()
            .insert(version, (store.clone(), staged_path.clone()));
        store
            .put(&staged_path, bytes.into())
            .await
            .map_err(TransactionError::from)?;
        let now_ms = Utc::now().timestamp_millis();

        // Ratify the version with the catalog.
        // TODO(live): confirm the `file_name` form UC expects (bare vs `_staged_commits/`-relative).
        let commit = Commit::new(version as i64, now_ms, file_name, file_size, now_ms);
        let request = UpdateTableRequest::new(
            vec![DeltaTableRequirement::AssertTableUuid { uuid: table_uuid }],
            vec![DeltaTableUpdate::AddCommit { commit }],
        )
        .map_err(|e| commit_failed(format!("invalid update_table request: {e}")))?;
        let target =
            TableIdentifier::new(self.catalog.as_str(), self.schema.as_str(), self.table.as_str());
        let update_client = self
            .client
            .delta_update_client()
            .map_err(|e| commit_failed(format!("UC update client init failed: {e}")))?;

        match update_client.update_table(&target, request).await {
            Ok(()) => {
                self.staged.lock().unwrap().remove(&version);
                Ok(CommitResponse::Committed)
            }
            Err(err) if is_version_conflict(&err) => {
                // Accepted orphan (write plan §7): the staged file loses to another writer.
                self.staged.lock().unwrap().remove(&version);
                Ok(CommitResponse::Conflict { version })
            }
            Err(err) => Err(commit_failed(format!("UC update_table failed: {err}"))),
        }
    }

    async fn abort(
        &self,
        version: Version,
        _payload: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        let staged = self.staged.lock().unwrap().remove(&version);
        if let Some((store, path)) = staged {
            // Best-effort: the version was never ratified, so drop the staged file.
            let _ = store.delete(&path).await;
        }
        Ok(())
    }

    fn payload_kind(&self) -> PayloadKind {
        PayloadKind::Bytes
    }
}

#[async_trait]
impl CommitCoordinator for UnityCommitCoordinator {
    async fn get_commits(&self) -> DeltaResult<CommitList> {
        let resp = self
            .client
            .delta_rest_client()?
            .load_table(&self.catalog, &self.schema, &self.table)
            .await
            .map_err(|e| DeltaTableError::Generic(format!("UC load_table failed: {e}")))?;

        let raw_max_version = resp
            .latest_table_version
            .or(resp.metadata.last_commit_version)
            .unwrap_or(-1);
        let max_version = if raw_max_version < 0 {
            0
        } else {
            raw_max_version as u64
        };

        Ok(CommitList {
            commits: resp.commits,
            max_version,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CatalogManagedLogStore<C: CommitCoordinator> {
    prefixed_store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    config: LogStoreConfig,
    coordinator: Arc<C>,
}

impl<C: CommitCoordinator> CatalogManagedLogStore<C> {
    pub fn new(
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        config: LogStoreConfig,
        coordinator: Arc<C>,
    ) -> Self {
        Self {
            prefixed_store,
            root_store,
            config,
            coordinator,
        }
    }
}

#[async_trait]
impl<C: CommitCoordinator + Committer + 'static> LogStore for CatalogManagedLogStore<C> {
    fn name(&self) -> String {
        "CatalogManagedLogStore".into()
    }

    fn committer(&self) -> Arc<dyn Committer> {
        self.coordinator.clone()
    }

    async fn read_commit_entry(&self, version: Version) -> DeltaResult<Option<Bytes>> {
        deltalake_core::logstore::read_commit_entry(self.prefixed_store.as_ref(), version).await
    }

    async fn get_latest_version(&self, _start_version: Version) -> DeltaResult<Version> {
        Ok(self.coordinator.get_commits().await?.max_version)
    }

    async fn catalog_log_tail(&self) -> DeltaResult<Option<CatalogLogTail>> {
        let list = self.coordinator.get_commits().await?;
        let mut base = self.config.location().clone();
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let mut log_tail = Vec::with_capacity(list.commits.len());
        let mut commits = list.commits;
        commits.sort_by_key(|c| c.version);
        for c in commits {
            let file_path = Path::parse(&c.file_name)?;
            let file_name = file_path.filename().unwrap_or(&c.file_name);
            let path = LogPath::staged_commit(
                base.clone(),
                file_name,
                c.file_modification_timestamp,
                c.file_size as u64,
            )
            .map_err(|e| {
                DeltaTableError::Generic(format!(
                    "invalid staged commit path for {}: {e}",
                    c.file_name
                ))
            })?;
            log_tail.push(path);
        }

        Ok(Some(CatalogLogTail {
            log_tail,
            max_version: list.max_version,
        }))
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.prefixed_store.clone()
    }

    fn root_object_store(&self) -> Arc<dyn ObjectStore> {
        self.root_store.clone()
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake_core::logstore::StorageConfig;
    use deltalake_core::logstore::object_store::memory::InMemory;
    use reqwest::Url;

    use futures::StreamExt;

    /// Test double: serves a fixed `CommitList` for reads and, as a `Committer`, writes staged
    /// files to its own in-memory store and reports a configurable commit outcome.
    #[derive(Debug)]
    struct MockCoordinator {
        list: CommitList,
        store: ObjectStoreRef,
        conflict: bool,
        staged: Mutex<HashMap<Version, (ObjectStoreRef, Path)>>,
    }

    impl MockCoordinator {
        fn new(list: CommitList) -> Self {
            Self {
                list,
                store: Arc::new(InMemory::new()),
                conflict: false,
                staged: Mutex::new(HashMap::new()),
            }
        }

        fn with_conflict(mut self, conflict: bool) -> Self {
            self.conflict = conflict;
            self
        }
    }

    #[async_trait]
    impl CommitCoordinator for MockCoordinator {
        async fn get_commits(&self) -> DeltaResult<CommitList> {
            Ok(self.list.clone())
        }
    }

    #[async_trait]
    impl Committer for MockCoordinator {
        fn payload_kind(&self) -> PayloadKind {
            PayloadKind::Bytes
        }

        async fn commit(
            &self,
            version: Version,
            payload: CommitOrBytes,
        ) -> Result<CommitResponse, TransactionError> {
            let bytes = match payload {
                CommitOrBytes::LogBytes(bytes) => bytes,
                CommitOrBytes::TmpCommit(_) => {
                    return Err(commit_failed("expected LogBytes".to_string()));
                }
            };
            let (_file_name, staged_path) = staged_commit_file(version, Uuid::new_v4());
            self.staged
                .lock()
                .unwrap()
                .insert(version, (self.store.clone(), staged_path.clone()));
            self.store
                .put(&staged_path, bytes.into())
                .await
                .map_err(TransactionError::from)?;
            if self.conflict {
                self.staged.lock().unwrap().remove(&version);
                Ok(CommitResponse::Conflict { version })
            } else {
                self.staged.lock().unwrap().remove(&version);
                Ok(CommitResponse::Committed)
            }
        }

        async fn abort(
            &self,
            version: Version,
            _payload: CommitOrBytes,
        ) -> Result<(), TransactionError> {
            let staged = self.staged.lock().unwrap().remove(&version);
            if let Some((store, path)) = staged {
                let _ = store.delete(&path).await;
            }
            Ok(())
        }
    }

    fn store() -> ObjectStoreRef {
        Arc::new(InMemory::new())
    }

    fn log_store(list: CommitList) -> CatalogManagedLogStore<MockCoordinator> {
        let url = Url::parse("memory:///cat.schema.table").unwrap();
        let config = LogStoreConfig::new(&url, StorageConfig::default());
        CatalogManagedLogStore::new(store(), store(), config, Arc::new(MockCoordinator::new(list)))
    }

    fn sample_commits() -> CommitList {
        CommitList {
            commits: vec![
                Commit::new(
                    1,
                    100,
                    "_staged_commits/00000000000000000001.1b1c9f7e-1234-5678-9012-345678901234.json",
                    10,
                    200,
                ),
                Commit::new(
                    2,
                    200,
                    "00000000000000000002.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json",
                    20,
                    200,
                ),
            ],
            max_version: 2,
        }
    }

    #[tokio::test]
    async fn get_latest_version_uses_catalog_max() {
        let ls = log_store(sample_commits());
        assert_eq!(ls.get_latest_version(0).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn catalog_log_tail_is_sorted_and_capped() {
        let ls = log_store(sample_commits());
        let tail = ls
            .catalog_log_tail()
            .await
            .unwrap()
            .expect("catalog-managed store must provide a log tail");
        assert_eq!(tail.max_version, 2);
        assert_eq!(tail.log_tail.len(), 2);

        let urls: Vec<String> = tail
            .log_tail
            .iter()
            .map(|lp| {
                let parsed: delta_kernel::path::ParsedLogPath = lp.clone().into();
                parsed.location.location.to_string()
            })
            .collect();
        assert!(urls[0].ends_with(
            "_delta_log/_staged_commits/00000000000000000001.1b1c9f7e-1234-5678-9012-345678901234.json"
        ));
        assert!(urls[1].ends_with(
            "_delta_log/_staged_commits/00000000000000000002.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json"
        ));
    }

    #[tokio::test]
    async fn empty_commits_yield_empty_tail() {
        let ls = log_store(CommitList {
            commits: vec![],
            max_version: 5,
        });
        let tail = ls.catalog_log_tail().await.unwrap().unwrap();
        assert_eq!(tail.max_version, 5);
        assert!(tail.log_tail.is_empty());
    }

    #[test]
    fn staged_commit_file_is_zero_padded_under_staged_commits() {
        let (file_name, path) = staged_commit_file(5, Uuid::nil());
        assert_eq!(
            file_name,
            "00000000000000000005.00000000-0000-0000-0000-000000000000.json"
        );
        assert_eq!(
            path.as_ref(),
            "_delta_log/_staged_commits/00000000000000000005.00000000-0000-0000-0000-000000000000.json"
        );
    }

    async fn staged_files(store: &ObjectStoreRef) -> Vec<String> {
        store
            .list(Some(&Path::from("_delta_log/_staged_commits")))
            .map(|res| res.unwrap().location.to_string())
            .collect()
            .await
    }

    #[tokio::test]
    async fn committer_writes_staged_file_and_reports_committed() {
        let mock = MockCoordinator::new(CommitList::default());
        assert_eq!(mock.payload_kind(), PayloadKind::Bytes);

        let resp = mock
            .commit(3, CommitOrBytes::LogBytes(Bytes::from_static(b"{}")))
            .await
            .unwrap();
        assert_eq!(resp, CommitResponse::Committed);

        let files = staged_files(&mock.store).await;
        assert_eq!(files.len(), 1);
        assert!(files[0].starts_with("_delta_log/_staged_commits/00000000000000000003."));
        assert!(files[0].ends_with(".json"));
    }

    #[tokio::test]
    async fn committer_reports_conflict() {
        let mock = MockCoordinator::new(CommitList::default()).with_conflict(true);
        let resp = mock
            .commit(3, CommitOrBytes::LogBytes(Bytes::from_static(b"{}")))
            .await
            .unwrap();
        assert_eq!(resp, CommitResponse::Conflict { version: 3 });
    }

    #[tokio::test]
    async fn abort_deletes_the_staged_file() {
        let mock = MockCoordinator::new(CommitList::default());
        // Simulate a hard-failed attempt that left a staged file behind.
        let (_file_name, path) = staged_commit_file(7, Uuid::new_v4());
        mock.store
            .put(&path, Bytes::from_static(b"{}").into())
            .await
            .unwrap();
        mock.staged
            .lock()
            .unwrap()
            .insert(7, (mock.store.clone(), path.clone()));

        mock.abort(7, CommitOrBytes::LogBytes(Bytes::new()))
            .await
            .unwrap();

        assert!(staged_files(&mock.store).await.is_empty());
    }

    #[tokio::test]
    async fn committer_rejects_tmp_commit_payload() {
        let mock = MockCoordinator::new(CommitList::default());
        let err = mock
            .commit(1, CommitOrBytes::TmpCommit(Path::from("x")))
            .await
            .unwrap_err();
        assert!(matches!(err, TransactionError::LogStoreError { .. }));
    }

    #[test]
    fn parse_uc_identity_splits_three_parts() {
        let url = Url::parse("uc://main.sales.orders").unwrap();
        assert_eq!(
            crate::parse_uc_identity(&url),
            Some(("main".into(), "sales".into(), "orders".into()))
        );
        assert_eq!(
            crate::parse_uc_identity(&Url::parse("uc://main.sales").unwrap()),
            None
        );
    }

    #[test]
    fn catalog_managed_opt_in_detected() {
        let mut cfg = StorageConfig::default();
        assert!(!crate::is_catalog_managed_requested(&cfg));
        cfg.raw.insert("catalog_managed".into(), "true".into());
        assert!(crate::is_catalog_managed_requested(&cfg));
    }
}

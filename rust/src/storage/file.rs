use std::pin::Pin;
use std::path::Path;

use chrono::DateTime;
use futures::{Stream, TryStreamExt};
use tokio::fs;
use tokio_stream::wrappers::ReadDirStream;

use super::{ObjectMeta, StorageBackend, StorageError};

#[derive(Default, Debug)]
pub struct FileStorageBackend {}

impl FileStorageBackend {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl StorageBackend for FileStorageBackend {
    fn join_path(
        &self,
        path: &str,
        path_to_join: &str,
    ) -> String {
        let new_path = Path::new(path);
        new_path
            .join(path_to_join)
            .into_os_string()
            .into_string()
            .unwrap()
    }

    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let attr = fs::metadata(path).await?;

        Ok(ObjectMeta {
            path: path.to_string(),
            modified: DateTime::from(attr.modified().unwrap()),
        })
    }

    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        fs::read(path).await.map_err(StorageError::from)
    }

    async fn list_objs<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + 'a>>, StorageError>
    {
        let readdir = ReadDirStream::new(fs::read_dir(path).await?);

        Ok(Box::pin(readdir.err_into().and_then(|entry| async move {
            Ok(ObjectMeta {
                path: String::from(entry.path().to_str().unwrap()),
                modified: DateTime::from(entry.metadata().await.unwrap().modified().unwrap()),
            })
        })))
    }
}

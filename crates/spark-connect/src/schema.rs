use crate::proto::create_delta_table::Column;
use crate::proto::spark::data_type::Kind;
use crate::proto::spark::{DataType, data_type};
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::schema::{ArrayType, MapType, PrimitiveType, StructField, StructType};

impl Into<Column> for StructField {
    fn into(self) -> Column {
        Column {
            name: self.name,
            data_type: Some(self.data_type.into()),
            nullable: self.nullable,
            ..Default::default()
        }
    }
}

impl Into<DataType> for KernelDataType {
    fn into(self) -> DataType {
        let kind = match self {
            KernelDataType::Primitive(p) => match p {
                PrimitiveType::String => Kind::String(data_type::String::default()),
                PrimitiveType::Long => Kind::Long(data_type::Long::default()),
                PrimitiveType::Integer => Kind::Integer(data_type::Integer::default()),
                PrimitiveType::Short => Kind::Short(data_type::Short::default()),
                PrimitiveType::Byte => Kind::Byte(data_type::Byte::default()),
                PrimitiveType::Float => Kind::Float(data_type::Float::default()),
                PrimitiveType::Double => Kind::Double(data_type::Double::default()),
                PrimitiveType::Boolean => Kind::Boolean(data_type::Boolean::default()),
                PrimitiveType::Binary => Kind::Binary(data_type::Binary::default()),
                PrimitiveType::Date => Kind::Date(data_type::Date::default()),
                PrimitiveType::Timestamp => Kind::Timestamp(data_type::Timestamp::default()),
                PrimitiveType::TimestampNtz => {
                    Kind::TimestampNtz(data_type::TimestampNtz::default())
                }
                PrimitiveType::Decimal(d) => Kind::Decimal(data_type::Decimal {
                    precision: Some(d.precision() as i32),
                    scale: Some(d.scale() as i32),
                    ..Default::default()
                }),
            },
            KernelDataType::Array(a) => Kind::Array(Box::new(a.into())),
            KernelDataType::Struct(s) => Kind::Struct(s.into()),
            KernelDataType::Map(m) => Kind::Map(Box::new(m.into())),
            KernelDataType::Variant(v) => Kind::Variant(v.into()),
        };
        DataType { kind: Some(kind) }
    }
}

impl Into<data_type::Map> for Box<MapType> {
    fn into(self) -> data_type::Map {
        data_type::Map {
            key_type: Some(Box::new(self.key_type.into())),
            value_type: Some(Box::new(self.value_type.into())),
            value_contains_null: self.value_contains_null,
            type_variation_reference: 0,
        }
    }
}

impl Into<data_type::Array> for Box<ArrayType> {
    fn into(self) -> data_type::Array {
        data_type::Array {
            element_type: Some(Box::new(self.element_type.into())),
            contains_null: self.contains_null,
            type_variation_reference: 0,
        }
    }
}

impl Into<data_type::StructField> for StructField {
    fn into(self) -> data_type::StructField {
        data_type::StructField {
            name: self.name,
            data_type: Some(self.data_type.into()),
            nullable: self.nullable,
            metadata: None,
        }
    }
}

impl Into<data_type::Variant> for Box<StructType> {
    fn into(self) -> data_type::Variant {
        data_type::Variant {
            type_variation_reference: 0,
        }
    }
}
impl Into<data_type::Struct> for Box<StructType> {
    fn into(self) -> data_type::Struct {
        data_type::Struct {
            fields: self.fields().cloned().map(Into::into).collect(),
            type_variation_reference: 0,
        }
    }
}

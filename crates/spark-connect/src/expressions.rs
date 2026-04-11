use crate::proto::spark::expression::literal::{
    Array as LitArray, Decimal, LiteralType, Map as LitMap, Struct,
};
use crate::proto::spark::expression::{Alias, ExprType, Literal};
use crate::proto::spark::{DataType, Expression};
use arrow::array::{Array, ArrayAccessor, AsArray};

use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datafusion_expr::expr;
use delta_kernel::engine::arrow_conversion::TryIntoKernel;

impl Into<Expression> for Expr {
    fn into(self) -> Expression {
        let expr_type = match self {
            Expr::Alias(expr::Alias {
                name,
                metadata,
                expr,
                ..
            }) => {
                let metadata: Option<String> = metadata
                    .map(|m| m.to_hashmap())
                    .and_then(|value| serde_json::to_string(&value).ok());

                let e: Expression = (*expr).into();
                let alias = Alias::builder()
                    .name(vec![name])
                    .maybe_metadata(metadata)
                    .expr(Box::new(e))
                    .build();

                Some(ExprType::Alias(Box::new(alias)))
            }
            Expr::Literal(v, md) => {
                let literal_type: LiteralType = v.into();
                Some(ExprType::Literal(Literal {
                    literal_type: Some(literal_type),
                }))
            }
            e => {
                tracing::warn!("Unknown expression encountered: {e:?}");
                None
            }
        };
        Expression::builder().maybe_expr_type(expr_type).build()
    }
}

fn array_value(arr: &dyn Array, idx: usize) -> LiteralType {
    match arr.data_type() {
        arrow::datatypes::DataType::Null => LiteralType::Null(arr.data_type().clone().into()),
        arrow::datatypes::DataType::Boolean => LiteralType::Boolean(arr.as_boolean().value(idx)),
        arrow::datatypes::DataType::Int8
        | arrow::datatypes::DataType::Int16
        | arrow::datatypes::DataType::UInt8
        | arrow::datatypes::DataType::UInt16 => LiteralType::Short(arr.as_primitive().value(idx)),

        arrow::datatypes::DataType::Int32
        | arrow::datatypes::DataType::Int64
        | arrow::datatypes::DataType::UInt32
        | arrow::datatypes::DataType::UInt64 => LiteralType::Integer(arr.as_primitive().value(idx)),

        arrow::datatypes::DataType::Float16 => LiteralType::Float(arr.as_primitive().value(idx)),
        arrow::datatypes::DataType::Float32 => LiteralType::Float(arr.as_primitive().value(idx)),
        arrow::datatypes::DataType::Float64 => LiteralType::Double(arr.as_primitive().value(idx)),
        arrow::datatypes::DataType::Timestamp(v, tz) => {
            LiteralType::Timestamp(arr.as_primitive().value(idx))
        }
        arrow::datatypes::DataType::Date32 => LiteralType::Date(arr.as_primitive().value(idx)),
        arrow::datatypes::DataType::Date64 => LiteralType::Date(arr.as_primitive().value(idx)),
        arrow::datatypes::DataType::Time32(_) => {
            LiteralType::Timestamp(arr.as_primitive().value(idx))
        }
        arrow::datatypes::DataType::Time64(_) => LiteralType::Date(arr.as_primitive().value(idx)),
        arrow::datatypes::DataType::Binary => {
            LiteralType::Binary(arr.as_binary().value(idx).to_vec())
        }
        arrow::datatypes::DataType::FixedSizeBinary(_) => {
            LiteralType::Binary(arr.as_binary().value(idx).to_vec())
        }
        arrow::datatypes::DataType::LargeBinary => {
            LiteralType::Binary(arr.as_binary().value(idx).to_vec())
        }
        arrow::datatypes::DataType::BinaryView => {
            LiteralType::Binary(arr.as_binary().value(idx).to_vec())
        }
        arrow::datatypes::DataType::Utf8 => {
            LiteralType::String(arr.as_string().value(idx).to_string())
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            LiteralType::String(arr.as_string().value(idx).to_string())
        }
        arrow::datatypes::DataType::Utf8View => {
            LiteralType::String(arr.as_string().value(idx).to_string())
        }
        arrow::datatypes::DataType::List(l) => {
            let arr = arr.as_list().value(idx);
            array_value(arr.as_ref(), idx)
        }
        arrow::datatypes::DataType::ListView(_) => {
            let arr = arr.as_list().value(idx);
            array_value(arr.as_ref(), idx)
        }
        arrow::datatypes::DataType::FixedSizeList(_, _) => {
            let arr = arr.as_list().value(idx);
            array_value(arr.as_ref(), idx)
        }
        arrow::datatypes::DataType::LargeList(_) => {
            let arr = arr.as_list().value(idx);
            array_value(arr.as_ref(), idx)
        }
        arrow::datatypes::DataType::LargeListView(_) => {
            let arr = arr.as_list().value(idx);
            array_value(arr.as_ref(), idx)
        }
        arrow::datatypes::DataType::Struct(s) => {
            let struct_values: Vec<_> = arr
                .as_struct()
                .columns()
                .into_iter()
                .map(|c| Literal {
                    literal_type: Some(array_value(c, 0)),
                })
                .collect();
            LiteralType::Struct(Struct {
                struct_type: Some(arr.data_type().clone().into()),
                elements: struct_values,
            })
        }

        arrow::datatypes::DataType::Decimal32(p, s) => LiteralType::Decimal(Decimal {
            value: arr.as_primitive().value(idx),
            precision: Some(*p as i32),
            scale: Some(*s as i32),
        }),
        arrow::datatypes::DataType::Decimal64(p, s) => LiteralType::Decimal(Decimal {
            value: arr.as_primitive().value(idx),
            precision: Some(*p as i32),
            scale: Some(*s as i32),
        }),
        arrow::datatypes::DataType::Decimal128(p, s) => LiteralType::Decimal(Decimal {
            value: arr.as_primitive().value(idx),
            precision: Some(*p as i32),
            scale: Some(*s as i32),
        }),
        arrow::datatypes::DataType::Decimal256(p, s) => LiteralType::Decimal(Decimal {
            value: arr.as_primitive().value(idx),
            precision: Some(*p as i32),
            scale: Some(*s as i32),
        }),
        arrow::datatypes::DataType::Map(k, v) => {
            let m = arr.as_map();

            LiteralType::Map(LitMap {
                key_type: Some(m.key_type().clone().into()),
                value_type: Some(m.value_type().clone().into()),
                keys: vec![],
                values: vec![],
            })
        }

        tpe => unimplemented!("Not implemented yet: {:?}", tpe),
    }
}

impl Into<LiteralType> for ScalarValue {
    fn into(self) -> LiteralType {
        match self {
            ScalarValue::Null => LiteralType::Null(self.data_type().into()),
            ScalarValue::Boolean(b) => LiteralType::Boolean(b.unwrap_or_default()),
            ScalarValue::Float16(f) => LiteralType::Float(f32::from(f.unwrap_or_default())),
            ScalarValue::Float32(f) => LiteralType::Float(f32::from(f.unwrap_or_default())),
            ScalarValue::Float64(f) => LiteralType::Double(f64::from(f.unwrap_or_default())),
            ScalarValue::Decimal32(d, p, s) => LiteralType::Decimal(
                Decimal::builder()
                    .value(d.unwrap_or_default().to_string())
                    .precision(p as i32)
                    .scale(s as i32)
                    .build(),
            ),
            ScalarValue::Decimal64(d, p, s) => LiteralType::Decimal(
                Decimal::builder()
                    .value(d.unwrap_or_default().to_string())
                    .precision(p as i32)
                    .scale(s as i32)
                    .build(),
            ),
            ScalarValue::Decimal128(d, p, s) => LiteralType::Decimal(
                Decimal::builder()
                    .value(d.unwrap_or_default().to_string())
                    .precision(p as i32)
                    .scale(s as i32)
                    .build(),
            ),
            ScalarValue::Decimal256(d, p, s) => LiteralType::Decimal(
                Decimal::builder()
                    .value(d.unwrap_or_default().to_string())
                    .precision(p as i32)
                    .scale(s as i32)
                    .build(),
            ),
            ScalarValue::Int8(i) => LiteralType::Short(i.unwrap_or_default() as i32),
            ScalarValue::Int16(i) => LiteralType::Short(i.unwrap_or_default() as i32),

            ScalarValue::Int32(i) => LiteralType::Integer(i.unwrap_or_default()),
            ScalarValue::Int64(i) => LiteralType::Long(i.unwrap_or_default()),
            ScalarValue::UInt8(i) => LiteralType::Short(i.unwrap_or_default() as i32),
            ScalarValue::UInt16(i) => LiteralType::Short(i.unwrap_or_default() as i32),
            ScalarValue::UInt32(i) => LiteralType::Integer(i.unwrap_or_default() as i32),
            ScalarValue::UInt64(i) => LiteralType::Long(i.unwrap_or_default() as i64),
            ScalarValue::Utf8(t) | ScalarValue::Utf8View(t) | ScalarValue::LargeUtf8(t) => {
                LiteralType::String(t.unwrap_or_default())
            }
            ScalarValue::Binary(b)
            | ScalarValue::BinaryView(b)
            | ScalarValue::LargeBinary(b)
            | ScalarValue::FixedSizeBinary(_, b) => LiteralType::Binary(b.unwrap_or_default()),
            ScalarValue::FixedSizeList(l) => {
                let a = LitArray::builder()
                    .element_type(l.data_type().into())
                    .elements(l.iter().map(Into::into).collect())
                    .build();
                LiteralType::Array(a)
            }
            ScalarValue::List(l) => {
                let a = LitArray::builder()
                    .element_type(l.data_type().into())
                    .elements(l.iter().map(Into::into).collect())
                    .build();
                LiteralType::Array(a)
            }
            ScalarValue::LargeList(l) => {
                let a = LitArray::builder()
                    .element_type(l.data_type().into())
                    .elements(l.iter().map(Into::into).collect())
                    .build();
                LiteralType::Array(a)
            }
            ScalarValue::Struct(l) => {
                let lit_array = l
                    .columns()
                    .into_iter()
                    .map(|c| c[0].into())
                    .collect::<Vec<LiteralType>>();

                let s = Struct::builder()
                    .struct_type(l.data_type().into())
                    .elements(lit_array)
                    .build();
                LiteralType::Struct(s)
            }
            ScalarValue::Map(m) => {
                let keys = m.keys().into_data().map(Into::into).collect();
                let values = m.values().into_data().map(Into::into).collect();
                let m = LitMap::builder()
                    .key_type(m.key_type().into())
                    .value_type(m.value_type().try_into_kernel().unwrap().into())
                    .keys(keys)
                    .values(values)
                    .build();
                LiteralType::Map(m)
            }
            ScalarValue::Date32(d) => LiteralType::Date(d.unwrap_or_default()),
            ScalarValue::Date64(d) => LiteralType::Date(d.unwrap_or_default().try_into().unwrap()),
            ScalarValue::Time32Second(s) => {
                LiteralType::Timestamp(s.unwrap_or_default() as i64 * 1000 * 1000)
            }
            ScalarValue::Time32Millisecond(s) => {
                LiteralType::Timestamp(s.unwrap_or_default() as i64 * 1000)
            }
            ScalarValue::Time64Microsecond(s) => LiteralType::Timestamp(s.unwrap_or_default()),
            ScalarValue::Time64Nanosecond(s) => {
                LiteralType::Timestamp(s.unwrap_or_default() / 1000)
            }
            // ScalarValue::TimestampSecond(s, t) => {}
            // ScalarValue::TimestampMillisecond(_, _) => {}
            // ScalarValue::TimestampMicrosecond(_, _) => {}
            // ScalarValue::TimestampNanosecond(_, _) => {}
            // ScalarValue::IntervalYearMonth(_) => {}
            // ScalarValue::IntervalDayTime(_) => {}
            // ScalarValue::IntervalMonthDayNano(_) => {}
            // ScalarValue::DurationSecond(_) => {}
            // ScalarValue::DurationMillisecond(_) => {}
            // ScalarValue::DurationMicrosecond(_) => {}
            // ScalarValue::DurationNanosecond(_) => {}
            // ScalarValue::Union(_, _, _) => {}
            // ScalarValue::Dictionary(_, _) => {}
            _ => LiteralType::Null,
        }
    }
}

use std::collections::HashMap;
use std::io;
use std::io::{Error, Result};
use std::sync::Arc;

use arrow::datatypes;

use crate::{proto, OrcError};

const COLUMN_ID: &str = "column_id";

pub(crate) fn read_schema(
    types: &Vec<proto::Type>,
    col_stats: &Vec<proto::ColumnStatistics>,
) -> Result<datatypes::Schema> {
    validate_proto_schema(types)?;

    // read root struct with all columns
    let (fields, _) = read_field("", 0, types, 0, 0, col_stats)?;

    Ok(datatypes::Schema::new(fields))
}

fn read_field<N: Into<String>>(
    name: N,
    type_idx: usize,
    types: &Vec<proto::Type>,
    depth: usize,
    column_index: usize,
    col_stats: &Vec<proto::ColumnStatistics>,
) -> Result<(Vec<datatypes::Field>, usize)> {
    let col_type = &types[type_idx];
    let stats = &col_stats[column_index];
    let name = name.into();

    match col_type.kind() {
        proto::r#type::Kind::List => {
            check_children_count(col_type, 1, &name)?;

            let (mut inner_type, next_type) = read_field(
                "inner",
                type_idx + 1,
                types,
                depth + 1,
                column_index,
                col_stats,
            )?;
            Ok((
                vec![datatypes::Field::new(
                    name,
                    datatypes::DataType::List(Arc::new(
                        inner_type.pop().expect("Inner type of list missed"),
                    )),
                    stats.has_null(),
                )
                .with_metadata(HashMap::from([(
                    COLUMN_ID.to_string(),
                    type_idx.to_string(),
                )]))],
                next_type,
            ))
        }
        proto::r#type::Kind::Map => {
            check_children_count(col_type, 2, &name)?;

            let (mut key_type, next_type) = read_field(
                "key",
                type_idx + 1,
                types,
                depth + 1,
                column_index,
                col_stats,
            )?;
            debug_assert_eq!(key_type.len(), 1);
            let (mut value_type, next_type) = read_field(
                "value",
                next_type,
                types,
                depth + 1,
                column_index,
                col_stats,
            )?;
            debug_assert_eq!(value_type.len(), 1);

            Ok((
                vec![datatypes::Field::new(
                    name,
                    datatypes::DataType::Map(
                        Arc::new(datatypes::Field::new(
                            "entries",
                            datatypes::DataType::Struct(arrow::datatypes::Fields::from(vec![
                                key_type.pop().expect("Key type of map missed"),
                                value_type.pop().expect("Value type of map missed"),
                            ])),
                            stats.has_null(),
                        )),
                        stats.has_null(),
                    ),
                    stats.has_null(),
                )
                .with_metadata(HashMap::from([(
                    COLUMN_ID.to_string(),
                    type_idx.to_string(),
                )]))],
                next_type,
            ))
        }
        proto::r#type::Kind::Struct => {
            has_children(col_type, &name)?;

            let mut subfields = Vec::with_capacity(col_type.subtypes.len());
            let mut next_type_id = 0;
            for (i, subtype_index) in col_type.subtypes.iter().enumerate() {
                let (mut subfield, next_type) = read_field(
                    &col_type.field_names[i],
                    *subtype_index as usize,
                    types,
                    depth + 1,
                    i,
                    col_stats,
                )?;
                next_type_id = next_type;
                debug_assert_eq!(subfield.len(), 1);
                subfields.push(subfield.pop().expect("Struct field type missed"));
            }

            if depth == 0 {
                // this is a top level struct which represent a schema itself, unwrap it
                Ok((subfields, next_type_id))
            } else {
                Ok((
                    vec![datatypes::Field::new(
                        name,
                        datatypes::DataType::Struct(arrow::datatypes::Fields::from(subfields)),
                        stats.has_null(),
                    )
                    .with_metadata(HashMap::from([(
                        COLUMN_ID.to_string(),
                        type_idx.to_string(),
                    )]))],
                    next_type_id,
                ))
            }
        }
        proto::r#type::Kind::Union => {
            has_children(col_type, &name)?;

            let mut subfields = Vec::with_capacity(col_type.subtypes.len());
            let mut next_type_id = type_idx + subfields.len() + 1;
            for (i, subtype_index) in col_type.subtypes.iter().enumerate() {
                let (mut subfield, next_type) = read_field(
                    &col_type.field_names[i],
                    *subtype_index as usize,
                    types,
                    depth + 1,
                    column_index,
                    col_stats,
                )?;
                next_type_id = next_type;
                debug_assert_eq!(subfield.len(), 1);
                subfields.push(subfield.pop().expect("Union field type missed"));
            }
            Ok((
                vec![datatypes::Field::new(
                    name,
                    datatypes::DataType::Union(
                        datatypes::UnionFields::new(0..col_type.subtypes.len() as i8, subfields),
                        datatypes::UnionMode::Dense,
                    ),
                    stats.has_null(),
                )
                .with_metadata(HashMap::from([(
                    COLUMN_ID.to_string(),
                    type_idx.to_string(),
                )]))],
                next_type_id,
            ))
        }
        proto::r#type::Kind::Varchar | proto::r#type::Kind::Char => {
            let metadata = HashMap::from([
                (
                    "maximum_length".to_string(),
                    col_type.maximum_length().to_string(),
                ),
                (COLUMN_ID.to_string(), type_idx.to_string()),
            ]);
            Ok((
                vec![
                    datatypes::Field::new(name, datatypes::DataType::Utf8, stats.has_null())
                        .with_metadata(metadata),
                ],
                type_idx + 1,
            ))
        }
        proto::r#type::Kind::Decimal => {
            if col_type.precision() > u8::MAX as u32 || col_type.scale() > i8::MAX as u32 {
                return Err(Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Unsupported decimal precision/scale: {}/{}",
                        col_type.precision(),
                        col_type.scale()
                    ),
                ));
            }

            Ok((
                vec![datatypes::Field::new(
                    name,
                    datatypes::DataType::Decimal128(
                        col_type.precision() as u8,
                        col_type.scale() as i8,
                    ),
                    stats.has_null(),
                )
                .with_metadata(HashMap::from([(
                    COLUMN_ID.to_string(),
                    type_idx.to_string(),
                )]))],
                type_idx + 1,
            ))
        }
        _ => Ok((
            vec![datatypes::Field::new(
                name,
                map_to_basic_arrow_datatype(col_type.kind())?,
                stats.has_null(),
            )
            .with_metadata(HashMap::from([(
                COLUMN_ID.to_string(),
                type_idx.to_string(),
            )]))],
            type_idx + 1,
        )),
    }
}

/// Extract index of column in ORC schema associated with this field.
/// Each ORC column has associated index in ORC file schema. This method extracts
/// this index from column metadata.
///
/// **Warn**: field must be created by [`read_schema`] method. Otherwise, it returns `Err`.
pub(crate) fn get_column_id(field: &arrow::datatypes::Field) -> crate::Result<u32> {
    field.metadata()[COLUMN_ID]
        .parse()
        .map_err(|_| OrcError::General(format!("Column index is not set for field {}", field)))
}

#[cfg(test)]
pub(crate) fn set_column_id(field: arrow::datatypes::Field, id: u32) -> arrow::datatypes::Field {
    field.with_metadata(HashMap::from([(COLUMN_ID.to_string(), id.to_string())]))
}

fn map_to_basic_arrow_datatype(r#type: proto::r#type::Kind) -> Result<datatypes::DataType> {
    match r#type {
        proto::r#type::Kind::Boolean => Ok(datatypes::DataType::Boolean),
        proto::r#type::Kind::Byte => Ok(datatypes::DataType::Int8),
        proto::r#type::Kind::Short => Ok(datatypes::DataType::Int16),
        proto::r#type::Kind::Int => Ok(datatypes::DataType::Int32),
        proto::r#type::Kind::Long => Ok(datatypes::DataType::Int64),
        proto::r#type::Kind::Float => Ok(datatypes::DataType::Float32),
        proto::r#type::Kind::Double => Ok(datatypes::DataType::Float64),
        proto::r#type::Kind::Binary => Ok(datatypes::DataType::Binary),
        proto::r#type::Kind::String => Ok(datatypes::DataType::Utf8),
        proto::r#type::Kind::Timestamp | proto::r#type::Kind::TimestampInstant => Ok(
            datatypes::DataType::Timestamp(datatypes::TimeUnit::Nanosecond, Some("UTC".into())),
        ),
        proto::r#type::Kind::Date => Ok(datatypes::DataType::Date64),
        _ => Err(Error::new(
            io::ErrorKind::InvalidInput,
            format!("Type {type:?} is not supported"),
        )),
    }
}

fn check_children_count<N: AsRef<str>>(
    field: &proto::Type,
    expected_children: usize,
    col_name: &N,
) -> Result<()> {
    let col_name = col_name.as_ref();
    let children = field.subtypes.len();
    let col_type = field.kind();
    if children != expected_children {
        return Err(Error::new(
            io::ErrorKind::InvalidInput,
            format!("Column '{col_name}' of <{col_type:?}> type has {children} children but {expected_children} expected."),
        ));
    }
    Ok(())
}

fn has_children<N: AsRef<str>>(field: &proto::Type, col_name: &N) -> Result<()> {
    let col_name = col_name.as_ref();
    let children = field.subtypes.len();
    let col_type = field.kind();
    if children == 0 {
        return Err(Error::new(
            io::ErrorKind::InvalidInput,
            format!("Column '{col_name}' of <{col_type:?}> must have at least one child."),
        ));
    }
    Ok(())
}

fn validate_proto_schema(types: &Vec<proto::Type>) -> Result<()> {
    // validate the schema
    if types.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ORC footer misses the schema(types vector is empty)",
        ));
    }

    if types[0].kind() != proto::r#type::Kind::Struct {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Top level type in ORC file must has '{}' type",
                proto::r#type::Kind::Struct.as_str_name()
            ),
        ));
    }

    // Schema tree should be numbered in increasing order, level by level.
    // Increase of type ID happens on each new type(going deeper when nested type is found).
    //
    // For instance, we have a schema:
    //               Struct(0)
    //         /         |      \
    //  Int(1)      Struct(2)  Float(5)
    //               |      \
    //          Int(3)      String(4)
    //
    // Types should be encoded in this way:
    // Type_ID: 0
    //      SubTypes: 1,2,5
    // Type_ID: 1
    // Type_ID: 2
    //      SubTypes: 3,4
    // Type_ID: 3
    // Type_ID: 4
    // Type_ID: 5
    let max_type_id = types.len();
    for (type_id, field_type) in types.iter().enumerate() {
        if field_type.kind() == proto::r#type::Kind::Struct
            && field_type.field_names.len() != field_type.subtypes.len()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Footer schema is corrupted: has {} field names and {} subtypes.",
                    field_type.field_names.len(),
                    field_type.subtypes.len(),
                ),
            ));
        }

        for subtype_idx in 0..field_type.subtypes.len() {
            let subtype_id = field_type.subtypes[subtype_idx];
            if subtype_id <= type_id as u32 {
                return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Subtype has ID >= than its holder type: subtype_id={subtype_id}, outer_type_id={type_id}",
                        ),
                    ));
            }
            if subtype_id >= max_type_id as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Invalid subtype ID={subtype_id}(should be less than max ID={max_type_id})",
                    ),
                ));
            }
            if subtype_idx > 0 && subtype_id < field_type.subtypes[subtype_idx - 1] {
                return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Invalid schema type order: types should be numbered in increasing order. \
                            Outer type ID={type_id}, subtype_id={subtype_id}, previous subtype_id={}",
                            field_type.subtypes[subtype_idx - 1],
                        ),
                    ));
            }
        }
    }

    Ok(())
}

use std::io;
use std::io::Result;

use crate::proto;

fn read_schema(types: &Vec<proto::Type>) -> Result<arrow::datatypes::Schema> {
    validate_proto_schema(types)?;

    let max_type_id = types.len();
    for type_id in 0..max_type_id {
        let field_type = &types[type_id];
        match field_type.kind() {
            proto::r#type::Kind::Int => {}
        }
    }

    Ok(arrow::datatypes::Schema::empty())
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
    for type_id in 0..max_type_id {
        let field_type = &types[type_id];
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

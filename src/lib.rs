pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/orc.proto.rs"));
}

mod io_utils;
mod schema;
mod stripe;
mod tail;
#[cfg(test)]
mod test;

pub mod column_reader;
pub mod compression;
pub mod reader;
pub mod source;
pub use reader::new_reader;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/orc.proto.rs"));
}

mod io_utils;

pub mod compression;
// pub mod reader;
// pub use reader::new_reader;

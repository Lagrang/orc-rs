use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut proto_files: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir("src/proto")? {
        let e = entry?;
        if e.file_type().unwrap().is_file() {
            proto_files.push(e.path());
        }
    }
    prost_build::Config::new()
        .format(true)
        .bytes(["."])
        .compile_protos(&proto_files, &["src/proto"])?;
    Ok(())
}

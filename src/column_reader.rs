pub struct ColumnReaderOptions {
    // Stripes are used to read the column.
    stripes: Vec<usize>,
}

pub trait ColumnReader {
    // Read next column chunk.
    //
    // Returns `None` if no more chunk left.
    fn read(&mut self) -> Option<arrow::array::ArrayRef>;
}

pub struct ColumnReaderBase {
    // pub file_reader
}

pub struct IntReader {}

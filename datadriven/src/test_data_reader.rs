// struct TestDataReader {
//     source_name: String,
//     reader
// }

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufRead, BufWriter, Write, Read};
use std::io;

#[test]
fn test_data() {
    let file = OpenOptions::new().read(true).open("src/testdata/data.txt").unwrap();
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).unwrap();
    assert_eq!(contents, "Hello, world!");
}
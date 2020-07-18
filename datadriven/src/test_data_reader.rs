use crate::errors::Result;
use crate::test_data::TestData;
use serde_json::Value;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read};

struct TestDataReader {
    source_name: String,
    data: TestData,
    buf: Vec<(usize, String)>,
}

impl TestDataReader {
    pub fn new(source_name: String, file: File) -> Self {
        let lines: Vec<String> = BufReader::new(file)
            .lines()
            .collect::<Result<_, _>>()
            .unwrap();

        let mut buf: Vec<(usize, String)> = vec![];
        let mut current_line = String::new();
        for (i, line) in lines.iter().enumerate() {
            let l = line.trim();
            if l.is_empty() || l.char_indices().next().unwrap().1 == '#' {
                // skip comment
                buf.push((i, l.to_string()));
            }
            if l.ends_with('\\') {
                current_line.push_str(l.trim_end());
                current_line.push(' ');
            } else {
                current_line.push_str(l);
            }
            // TODO(accelsao): start of line or end of line
            buf.push((i, current_line));
            current_line = String::new();
        }

        Self {
            source_name,
            buf,
            data: Default::default(),
        }
    }
    pub fn next(&mut self) -> bool {
        for (i, line) in self.buf.iter() {
            // Ensure to not re-initialize r.data unless a line is read
            // successfully. The reason is that we want to keep the last
            // stored value of `Pos` after encountering EOF, to produce useful
            // error messages.
            self.data = TestData::default();

            // Update Pos early so that a late error message has an updated
            // position.
            self.data.pos = format!("{}:{}", self.source_name, i);
            // let line = line.trim();
        }
        true
    }
}


#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{BufReader, Read, BufRead};
    use crate::errors::Result;
    use std::fs;
    use serde_json::Value;
    use std::collections::HashMap;
    use serde::{Serialize, Deserialize};


    #[derive(Debug, Deserialize)]
    struct Configuration {
        incoming: Vec<u64>,
        outgoing: Vec<u64>,
        learners: Vec<u64>,
        learners_next: Vec<u64>,
        auto_leave: bool,
    }

    #[derive(Debug, Deserialize)]
    struct Progress {
        state: String,
        matched: u64,
        next_idx: u64,
    }

    #[derive(Debug, Deserialize)]
    struct Expected {
        configuration: Configuration,
        progress: Progress,
    }

    #[derive(Debug, Deserialize)]
    struct MyValue {
        expected: Expected,
        argument: Vec<String>,
        command_type: String,
    }

    #[derive(Debug, Deserialize)]
    struct Apple {
        color: String,
        sweet: u32,
    }

    #[derive(Debug,Deserialize)]
    struct Person {
        name: String,
        age: u8,
        apples: Vec<Apple>,
    }

    // struct Configuration {
    //     incoming: Vec<u32>,
    //     outgoing: Vec<u32>,
    //
    // }
    //
    // struct TestCase {
    //     command_type: String,
    //     argument: Vec<HashMap<String, u32>>,
    //     expected:
    // }

    // TODO: Remove Belows
    #[test]
    fn test_data() {
        let file = OpenOptions::new()
            .read(true)
            .open("src/testdata/data.txt")
            .unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents).unwrap();
        assert_eq!(2, 1 + 1);
    }

    #[test]
    fn test_read_lines() -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .open("src/testdata/data.txt")
            .unwrap();
        let buf = BufReader::new(file);
        for (i, line) in buf.lines().enumerate() {
            println!("{}:{}", i, line.unwrap());
        }
        Ok(())
    }

    #[test]
    fn test_lines() {
        let file = OpenOptions::new()
            .read(true)
            .open("src/testdata/data.txt")
            .unwrap();
        let a: Vec<_> = BufReader::new(file)
            .lines()
            .collect::<Result<_, _>>()
            .unwrap();
    }

    #[test]
    fn test_json() -> Result<()> {
        let data = fs::read_to_string("src/testdata/data.json")?;

        let v: Value = serde_json::from_str(&data)?;
        println!("comment = {:?}", v["test-case"][0]["comment"]);
        println!("command_type = {:?}", v["test-case"][0]["command_type"]);
        println!("incoming = {:?}", v["test-case"][0]["expected"]["Configuration"]["incoming"]);
        println!("expected = {:?}", v["test-case"][0]["expected"]);
        println!("Configuration = {:?}", v["test-case"][0]["expected"]["Configuration"]);
        Ok(())
    }

    #[test]
    fn test_json_struct() -> Result<()> {
        let data = fs::read_to_string("src/testdata/test.json")?;

        let v: MyValue = serde_json::from_str(&data)?;
        println!("value = {:?}", v);
        Ok(())
    }
}
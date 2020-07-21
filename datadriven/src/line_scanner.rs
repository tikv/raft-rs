use nom::lib::std::fmt::Formatter;
use std::fmt::Debug;
use std::str::Lines;

pub struct LineScanner<'a> {
    pub line: u32,
    scanner: Lines<'a>,
}

impl<'a> LineScanner<'a> {
    pub fn new(text: &'a str) -> Self {
        Self {
            line: 0,
            scanner: text.lines(),
        }
    }

    pub fn scan(&mut self) -> Option<&str> {
        self.line += 1;
        self.scanner.next()
    }
}

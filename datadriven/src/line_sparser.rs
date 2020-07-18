use crate::errors::{Error, Result};
use crate::test_data::CmdArg;
use regex::Regex;

// token
// (1) argument
// (2) argument=a,b,c
// (3) argument=
// (4) argument=(a,b,c,...)
// (5) a,b,c

pub fn parse_line(line: String) -> Result<(String, Vec<CmdArg>)> {
    Ok((String::new(), vec![]))
}

fn split_directives(line: String) -> Result<Vec<String>> {
    let mut res = vec![];
    let origin_line = line.clone();

    let re = Regex::new(
        r"^ *[-a-zA-Z0-9/_,.]+(=[-a-zA-Z0-9_@=+/,.]*|=\([-a-zA-Z0-9_@=+/,.]*\)| *)( |$)",
    )
    .unwrap();

    let mut line = line;
    while !line.is_empty() {
        let l = re.captures(&line);
        if let Some(l) = re.captures(&line) {
            let str = &l[0];
            let (first, last) = line.split_at(str.len());
            res.push(first.trim().to_string());
            line = last.to_string();
        } else {
            let col = origin_line.len() - line.len() + 1;
            return Err(Error::msg(format!(
                "cannot parse directive at column {}: {}",
                col, origin_line
            )));
        }
    }
    Ok(res)
}

// TODO: Remove Belows

#[test]
fn test_re() {
    let res = split_directives(String::from("      a=123,abc,a   b=532,12   c=(3,2,9)")).unwrap();
    assert_eq!(res, vec!["a=123,abc,a", "b=532,12", "c=(3,2,9)"]);
    let res = split_directives(String::from("p a=1 b=(3,5) c=2,3")).unwrap();
    assert_eq!(res, vec!["p", "a=1", "b=(3,5)", "c=2,3"]);
}

#[test]
#[should_panic]
fn test_re_fails() {
    let res = split_directives(String::from("p a= b=(3 c=1,2")).unwrap();
}

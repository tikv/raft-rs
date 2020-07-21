use crate::errors::{Error, Result};
use crate::test_data::CmdArg;
use regex::Regex;

// token
// (1) argument
// (2) argument=a,b,c
// (3) argument=
// (4) argument=(a,b,c,...)
// (5) a,b,c

pub fn parse_line(line: &str) -> Result<(String, Vec<CmdArg>)> {
    let field = split_directives(line)?;
    if field.is_empty() {
        return Ok((String::new(), Vec::new()));
    }

    let mut cmd_args = vec![];

    let cmd = field.first().unwrap().as_str().to_string();
    for (i, arg) in field[1..].iter().enumerate() {
        let v: Vec<String> = arg.split_terminator('=').map(|v| v.to_string()).collect();
        assert_eq!(v.len(), 2, "we should get two string seperated by '='.");
        let (key, value) = (v[0].clone(), v[1].clone());

        let mut values: Vec<String>;
        if value.len() > 2 && value.starts_with('(') && value.ends_with(')') {
            values = value[1..value.len() - 1]
                .split_terminator('.')
                .map(|v| v.to_string())
                .collect();
            values = values.into_iter().map(|v| v.trim().to_string()).collect();
        } else {
            values = value.split_terminator('.').map(|v| v.to_string()).collect();
            values = values.into_iter().map(|v| v.trim().to_string()).collect();
        }
        cmd_args.push(CmdArg { key, values })
    }

    Ok((cmd, cmd_args))
}

lazy_static! {
    static ref RE: Regex = Regex::new(
        r"^ *[-a-zA-Z0-9/_,.]+(=[-a-zA-Z0-9_@=+/,.]*|=\([^)]*\)| *)( |$)"
    )
    .unwrap();
}

fn split_directives(line: &str) -> Result<Vec<String>> {
    let mut res = vec![];
    let origin_line = line.clone();

    let mut line = line.clone();
    while !line.is_empty() {
        println!("line: {:?}", line);
        if let Some(l) = RE.captures(&line) {
            let str = &l[0];
            let (first, last) = line.split_at(str.len());
            res.push(first.trim().to_string());
            line = last
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_re() {
        // let res = split_directives("      a=123,abc,a   b=532,12   c=(3,2,9)".into()).unwrap();
        // assert_eq!(res, vec!["a=123,abc,a", "b=532,12", "c=(3,2,9)"]);
        // let res = split_directives("      a=123,abc,a   b=532,12   c=(3,2,9)".into()).unwrap();
        // assert_eq!(res, vec!["p", "a=1", "b=(3,5)", "c=2,3"]);
    }

    #[test]
    #[should_panic]
    fn test_re_fails() {
        // let res = split_directives("p a= b=(3 c=1,2".into()).unwrap();
    }
}

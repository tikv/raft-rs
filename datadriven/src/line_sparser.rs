use crate::test_data::CmdArg;
use anyhow::{Error, Result};
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
        bail!("empty lines occurs, unexpected.");
    }

    let cmd = field[0].clone();
    let mut cmd_args = vec![];

    for arg in field[1..].iter() {
        let vs: Vec<String> = arg.split_terminator('=').map(|v| v.to_string()).collect();
        if vs.len() == 1 {
            cmd_args.push(CmdArg {
                key: vs[0].clone(),
                values: vec![],
            })
        } else if vs.len() == 2 {
            let (key, value) = (vs[0].clone(), vs[1].clone());

            debug!("key: {}, value: {}", key, value);
            let mut values: Vec<String>;
            if value.starts_with('(') && value.ends_with(')') {
                values = value[1..value.len() - 1]
                    .split_terminator(',')
                    .map(|v| v.to_string())
                    .collect();
                values = values.into_iter().map(|v| v.trim().to_string()).collect();
            } else {
                let p: Vec<&str> = value.split_terminator('.').collect();
                debug!("p: {:?}", p);
                values = value.split_terminator(',').map(|v| v.to_string()).collect();
                debug!("values: {:?}, len: {}", values, values.len());
                values = values.into_iter().map(|v| v.trim().to_string()).collect();
            }
            debug!("values: {:?}", values);
            cmd_args.push(CmdArg { key, values })
        } else {
            bail!("unknown argument format: {}", arg)
        }
    }

    Ok((cmd, cmd_args))
}

lazy_static! {
    static ref RE: Regex =
        Regex::new(r"^ *[-a-zA-Z0-9/_,.]+(=[-a-zA-Z0-9_@=+/,.]*|=\([^)]*\)| *)( |$)").unwrap();
}

fn split_directives(line: &str) -> Result<Vec<String>> {
    let mut res = vec![];

    let origin_line = line.to_string();
    let mut line = line.to_string();

    while !line.is_empty() {
        if let Some(l) = RE.captures(&line) {
            let str = &l[0];
            let (first, last) = line.split_at(str.len());
            res.push(first.trim().to_string());
            line = last.to_string();
        } else {
            let col = origin_line.len() - line.len() + 1;
            return Err(Error::msg(format!(
                "cannot parse directive at column {}: '{}'",
                col, origin_line
            )));
        }
    }
    Ok(res)
}

#[cfg(test)]
mod tests {
    use crate::line_sparser::split_directives;
    use anyhow::Result;

    fn init() -> Result<()> {
        // TODO(accelsao): is there any way to init once instead of inserting to every test?
        let _ = env_logger::builder().is_test(true).try_init();
        Ok(())
    }

    #[test]
    fn test_re() -> Result<()> {
        init()?;
        let line = "cmd a=1 b=2,2,2 c=(3,33,3333)";
        assert_eq!(
            format!("{:?}", split_directives(line)?),
            "[\"cmd\", \"a=1\", \"b=2,2,2\", \"c=(3,33,3333)\"]"
        );
        let line = "cmd                           a=11 b=2,2,2 cc=(3, 2, 1)";
        assert_eq!(
            format!("{:?}", split_directives(line)?),
            "[\"cmd\", \"a=11\", \"b=2,2,2\", \"cc=(3, 2, 1)\"]"
        );
        let line = "cmd       \n               a=11 b=2,2,2 cc=(3, 2, 1)";
        assert!(split_directives(line).is_err());
        let line = "cmd \\ a=11 \\ b=2,2,2 \n cc=(3, 2, 1)";
        assert!(split_directives(line).is_err());
        Ok(())
    }
}

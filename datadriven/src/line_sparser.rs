use crate::test_data::CmdArg;
use anyhow::Result;
use regex::Regex;

// token
// (1) argument
// (2) argument=a,b,c
// (3) argument=
// (4) argument=(a,b,c,...)
// (5) a,b,c
pub fn parse_line(line: &str, logger: &slog::Logger) -> Result<(String, Vec<CmdArg>)> {
    let field = split_directives(line)?;
    if field.is_empty() {
        bail!("empty lines occurs, unexpected.");
    }

    debug!(logger, "argument after split: {:?}", field);

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
            let mut values: Vec<String>;
            if value.starts_with('(') && value.ends_with(')') {
                values = value[1..value.len() - 1]
                    .split_terminator(',')
                    .map(|v| v.to_string())
                    .collect();
            } else {
                values = value.split_terminator(',').map(|v| v.to_string()).collect();
            }
            values = values.into_iter().map(|v| v.trim().to_string()).collect();
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

    let mut line = line.to_string();

    while !line.is_empty() {
        if let Some(l) = RE.captures(&line) {
            // get first captures
            let (first, last) = line.split_at(l[0].len());
            res.push(first.trim().to_string());
            line = last.to_string();
        } else {
            bail!("cant parse argument: '{}'", line)
        }
    }
    Ok(res)
}

#[cfg(test)]
mod tests {
    use crate::default_logger;
    use crate::line_sparser::{parse_line, split_directives};
    use anyhow::Result;

    #[test]
    fn test_parse_line() -> Result<()> {
        let logger = default_logger();
        let line = "cmd a=1 b=(2,3) c= d";
        let (cmd, cmd_args) = parse_line(line, &logger)?;
        assert_eq!(cmd, "cmd");
        assert_eq!(format!("{:?}", cmd_args), "[a=\"1\", b=\"2,3\", c, d]");

        Ok(())
    }

    #[test]
    fn test_split_directives() -> Result<()> {
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

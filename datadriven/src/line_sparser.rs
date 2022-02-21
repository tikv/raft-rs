use crate::test_data::CmdArg;
use anyhow::Result;
use regex::Regex;

use anyhow::{anyhow, bail};
use lazy_static::lazy_static;
use slog::debug;

// Token
// (1) argument (no value)
// (2) argument= (empty value)
// (3) argument=() (empty value)
// (4) argument=a (single value)
// (5) argument=a,b,c (single value)
// (6) argument=(a,b,c,...) (multiple value)
//
// parse_line parses a line of datadriven input language and returns
// the parsed command and CmdArgs.
pub fn parse_line(line: &str, logger: &slog::Logger) -> Result<(String, Vec<CmdArg>)> {
    debug!(logger, "line pass to split_directives: {:?}", line);

    let fields = split_directives(line)?;
    if fields.is_empty() {
        return Ok((String::new(), vec![]));
    }

    debug!(logger, "argument after split: {:?}", fields);

    let cmd = fields[0].clone();
    let mut cmd_args = vec![];

    for arg in &fields[1..] {
        let key_value = arg.splitn(2, '=').collect::<Vec<&str>>();

        debug!(logger, "keyvalue: {:?}", key_value);

        match key_value.len() {
            1 => {
                // key only
                cmd_args.push(CmdArg {
                    key: key_value[0].to_string(),
                    vals: vec![],
                })
            }
            2 => {
                let (key, val) = (key_value[0].to_string(), key_value[1]);

                debug!(logger, "val: {:?}", val);

                if val.starts_with('(') && val.ends_with(')') {
                    // trim because white space is allow.
                    let vals = val[1..val.len() - 1]
                        .split(',')
                        .map(|v| v.trim().to_string())
                        .collect();
                    cmd_args.push(CmdArg { key, vals })
                } else {
                    cmd_args.push(CmdArg {
                        key,
                        vals: vec![val.to_string()],
                    })
                }
            }
            _ => bail!("unknown argument format: {}", arg),
        }
    }

    Ok((cmd, cmd_args))
}

lazy_static! {
    static ref RE: Regex =
        Regex::new(r"^ *[-a-zA-Z0-9/_,.]+(|=[-a-zA-Z0-9_@=+/,.]*|=\([^)]*\))( |$)").unwrap();
}

fn split_directives(line: &str) -> Result<Vec<String>> {
    let mut res = vec![];

    let origin_line = <&str>::clone(&line);

    let mut line = line;
    while !line.is_empty() {
        if let Some(l) = RE.captures(line) {
            // get first captures
            let (first, last) = line.split_at(l[0].len());
            res.push(first.trim().to_string());
            line = last;
        } else {
            return Err(anyhow!(
                "cannot parse directive at column {}: {}",
                origin_line.len() - line.len() + 1,
                origin_line
            ));
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
        assert_eq!(format!("{:?}", cmd_args), "[a=1, b=(2,3), c=, d]");

        Ok(())
    }

    #[test]
    fn test_split_directives() -> Result<()> {
        let line = "cmd a=1 b=2,2,2 c=(3,33,3333)";
        assert_eq!(
            split_directives(line)?,
            ["cmd", "a=1", "b=2,2,2", "c=(3,33,3333)"],
        );

        let line = "cmd a b c";
        assert_eq!(split_directives(line)?, ["cmd", "a", "b", "c"]);

        let line = "cmd";
        assert_eq!(split_directives(line)?, ["cmd"]);

        let line = "cmd a=1\n";
        assert_eq!(
            split_directives(line).unwrap_err().to_string(),
            "cannot parse directive at column 5: cmd a=1\n".to_string()
        );

        let line = "cmd a=1 ";
        assert_eq!(split_directives(line)?, ["cmd", "a=1"]);

        let line = "cmd a=1  ";
        assert_eq!(
            split_directives(line).unwrap_err().to_string(),
            "cannot parse directive at column 9: cmd a=1  ".to_string()
        );

        Ok(())
    }
}

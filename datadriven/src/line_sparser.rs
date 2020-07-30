use crate::test_data::CmdArg;
use anyhow::Result;
use regex::Regex;

// Token
// (1) argument (no value)
// (2) argument= (no value)
// (3) argument=a (single value)
// (4) argument=a,b,c (multiple value)
// (5) argument=(a,b,c,...) (multiple value)
//
// parse_line parses a line of datadriven input language and returns
// the parsed command and CmdArgs.
pub fn parse_line(line: &str, logger: &slog::Logger) -> Result<(String, Vec<CmdArg>)> {
    let fields = split_directives(line)?;
    if fields.is_empty() {
        bail!("empty lines occurs, unexpected.");
    }

    debug!(logger, "argument after split: {:?}", fields);

    let cmd = fields[0].clone();
    let mut cmd_args = vec![];

    for arg in fields[1..].iter() {
        let vals: Vec<String> = arg.split_terminator('=').map(|v| v.to_string()).collect();

        if vals.len() == 1 {
            cmd_args.push(CmdArg {
                key: vals[0].clone(),
                vals: vec![],
            })
        } else if vals.len() == 2 {
            let (key, val) = (vals[0].clone(), vals[1].clone());

            let vals: Vec<String>;

            if val.starts_with('(') && val.ends_with(')') {
                vals = val[1..val.len() - 1]
                    .split_terminator(',')
                    .map(|v| v.trim().to_string())
                    .collect();
            } else {
                vals = val.split_terminator(',').map(|v| v.to_string()).collect();
            }

            cmd_args.push(CmdArg { key, vals })
        } else {
            bail!("unknown argument format: {}", arg)
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
    use crate::line_sparser::{parse_line, split_directives};
    use anyhow::Result;
    use slog::Drain;

    fn default_logger() -> slog::Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }

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

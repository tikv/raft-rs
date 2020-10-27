use crate::tracker::Configuration;
use crate::{default_logger, Changer, MapChange, ProgressTracker};
use datadriven::{run_test, walk};
use raft_proto::parse_conf_change;

#[test]
fn test_conf_change_data_driven() -> anyhow::Result<()> {
    walk("src/confchange/testdata", |path| -> anyhow::Result<()> {
        let logger = default_logger();

        let mut tr = ProgressTracker::new(10, default_logger());
        let mut c = Changer::new(&tr);
        let mut idx = 0;

        run_test(
            path.to_str().unwrap(),
            |data| -> String {
                let ccs = parse_conf_change(&data.input).unwrap();
                let mut cfg = Configuration::default();
                let mut prs = MapChange::default();

                match data.cmd.as_str() {
                    "simple" => {
                        if let (conf, changes) = c.simple(&ccs).unwrap() {
                            tr.apply_conf(conf, changes, idx);
                        }
                    }
                    "enter-joint" => {
                        let mut autoleave = false;
                        for arg in &data.cmd_args {
                            match arg.key.as_str() {
                                "autoleave" => {
                                    for val in &arg.vals {
                                        autoleave = val
                                            .parse()
                                            .expect("type of autoleave should be boolean")
                                    }
                                }
                                _ => {
                                    panic!("unknown arg: {}", arg.key);
                                }
                            }
                        }

                        if let (cfg, prs) = c.enter_joint(autoleave, &ccs).unwrap() {
                            tr.apply_conf(cfg, prs, idx);
                        }
                    }
                    _ => {
                        panic!("unknown arg: {}", data.cmd);
                    }
                }
                for (id, a) in prs {
                    println!("{}: ", id);
                }
                idx += 1;
                String::from(format!("{:?}\n", cfg))
            },
            false,
            &logger,
        )
    });
    Ok(())
}

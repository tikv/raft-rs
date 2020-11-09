use crate::{default_logger, Changer, ProgressTracker};
use datadriven::{run_test, walk};
use raft_proto::parse_conf_change;

#[test]
fn test_conf_change_data_driven() -> anyhow::Result<()> {
    walk("src/confchange/testdata", |path| -> anyhow::Result<()> {
        let logger = default_logger();

        let mut tr = ProgressTracker::new(10, default_logger());
        // let mut c = Changer::new(&mut tr);

        let mut idx = 0;

        run_test(
            path.to_str().unwrap(),
            |data| -> String {
                let ccs = parse_conf_change(&data.input).unwrap();
                // let mut cfg = Configuration::default();
                // let mut prs = MapChange::default();

                match data.cmd.as_str() {
                    "simple" => match Changer::new(&mut tr).simple(&ccs) {
                        Ok((conf, changes)) => {
                            tr.apply_conf(conf, changes, idx);
                        }
                        Err(e) => return e.to_string(),
                    },
                    "enter-joint" => {
                        let mut auto_leave = false;
                        for arg in &data.cmd_args {
                            match arg.key.as_str() {
                                "autoleave" => {
                                    for val in &arg.vals {
                                        auto_leave = val
                                            .parse()
                                            .expect("type of autoleave should be boolean")
                                    }
                                }
                                _ => {
                                    panic!("unknown arg: {}", arg.key);
                                }
                            }
                        }
                        match Changer::new(&mut tr).enter_joint(auto_leave, &ccs) {
                            Ok((conf, changes)) => {
                                tr.apply_conf(conf, changes, idx);
                            }
                            Err(e) => return e.to_string(),
                        }
                    }
                    _ => {
                        panic!("unknown arg: {}", data.cmd);
                    }
                }
                idx += 1;

                let mut buffer = String::new();

                let conf = tr.conf();
                buffer.push_str(&format!("{}\n", conf));

                println!("conf: {:?}", conf);
                // println!("voters: {:?}", voters);
                // println!("")

                let prs = tr.progress();

                for (k, v) in prs.iter() {
                    buffer.push_str(&format!(
                        "{}: {} match={} next={}\n",
                        k, v.state, v.matched, v.next_idx
                    ));
                }

                // println!("prs: {:?}", prs);
                // buffer.push_str(&format!("{}\n", prs));

                // for (id, a) in prs {
                //     println!("{}: {:?}", id, a);
                // }
                // println!("{:?}", );
                // String::from("123")
                // println!("{}", prs);
                buffer
            },
            false,
            &logger,
        )
    });
    Ok(())
}

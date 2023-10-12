use alloc::string::String;
use alloc::string::ToString;
use core::fmt::Write;

use crate::{default_logger, Changer, ProgressTracker};
use datadriven::{run_test, walk};
use itertools::Itertools;
use raft_proto::parse_conf_change;

#[test]
fn test_conf_change_data_driven() -> anyhow::Result<()> {
    walk("src/confchange/testdata", |path| -> anyhow::Result<()> {
        let logger = default_logger();

        let mut tr = ProgressTracker::new(10);
        let mut idx = 0;

        run_test(
            path.to_str().unwrap(),
            |data| -> String {
                let ccs = parse_conf_change(&data.input).unwrap();

                let res = match data.cmd.as_str() {
                    "simple" => Changer::new(&tr).simple(&ccs),
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
                        Changer::new(&tr).enter_joint(auto_leave, &ccs)
                    }
                    "leave-joint" => {
                        assert!(data.cmd_args.is_empty());
                        Changer::new(&tr).leave_joint()
                    }
                    _ => {
                        panic!("unknown arg: {}", data.cmd);
                    }
                };
                match res {
                    Ok((conf, changes)) => {
                        tr.apply_conf(conf, changes, idx);
                        idx += 1;
                    }
                    Err(e) => {
                        idx += 1;
                        return e.to_string();
                    }
                }

                let mut buffer = String::new();

                let conf = tr.conf();
                writeln!(buffer, "{}", conf).unwrap();

                let prs = tr.progress();

                // output with peer_id sorted
                for (k, v) in prs.iter().sorted_by(|&(k1, _), &(k2, _)| k1.cmp(k2)) {
                    write!(
                        buffer,
                        "{}: {} match={} next={}",
                        k, v.state, v.matched, v.next_idx
                    )
                    .unwrap();
                    if conf.learners.contains(k) {
                        buffer.push_str(" learner");
                    }
                    buffer.push('\n');
                }
                buffer
            },
            false,
            &logger,
        )
    })
}

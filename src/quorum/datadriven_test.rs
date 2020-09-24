use crate::quorum::{AckIndexer, AckedIndexer, Index};
use crate::{default_logger, HashMap, HashSet, JointConfig, MajorityConfig};
use datadriven::{run_test, TestData};

fn test_quorum(data: &TestData) -> String {
    // Two majority configs. The first one is always used (though it may
    // be empty) and the second one is used iff joint is true.
    let mut joint = false;
    let mut ids: Vec<u64> = Vec::new();
    let mut idsj: Vec<u64> = Vec::new();

    // The committed indexes for the nodes in the config in the order in
    // which they appear in (ids,idsj), without repetition. An underscore
    // denotes an omission (i.e. no information for this voter); this is
    // different from 0.
    //
    // For example,
    // cfg=(1,2) cfgj=(2,3,4) idxs=(_,5,_,7) initializes the idx for voter 2
    // to 5 and that for voter 4 to 7 (and no others).
    //
    // cfgj=zero is specified to instruct the test harness to treat cfgj
    // as zero instead of not specified (i.e. it will trigger a joint
    // quorum test instead of a majority quorum test for cfg only).
    let mut idxs: Vec<Index> = Vec::new();

    // Votes. These are initialized similar to idxs except the only values
    // used are 1 (voted against) and 2 (voted for). This looks awkward,
    // but is convenient because it allows sharing code between the two.
    let mut votes: Vec<Index> = Vec::new();

    for arg in &data.cmd_args {
        for val in &arg.vals {
            match arg.key.as_str() {
                "cfg" => {
                    let n: u64 = val.parse().expect("type of n should be u64");
                    ids.push(n);
                }
                "cfgj" => {
                    joint = true;

                    if val == "zero" {
                        assert_eq!(arg.vals.len(), 1, "cannot mix 'zero' into configuration")
                    } else {
                        let n: u64 = val.parse().expect("type of n should be u64");
                        idsj.push(n);
                    }
                }
                "idx" => {
                    let mut n: u64 = 0;
                    if val != "_" {
                        n = val.parse().expect("type of n should be u64");
                        if n == 0 {
                            panic!("use '_' as 0, check {}", data.pos)
                        }
                    }
                    idxs.push(Index {
                        index: n,
                        group_id: 0,
                    });
                }
                "votes" => match val.as_str() {
                    "y" => votes.push(Index {
                        index: 2,
                        group_id: 0,
                    }),
                    "n" => votes.push(Index {
                        index: 1,
                        group_id: 0,
                    }),
                    "_" => votes.push(Index {
                        index: 0,
                        group_id: 0,
                    }),
                    _ => {
                        panic!("unknown arg: {}", val);
                    }
                },
                _ => {
                    panic!("unknown arg: {}", arg.key);
                }
            }
        }
    }

    let ids_set: HashSet<u64> = ids.iter().cloned().collect();
    let idsj_set: HashSet<u64> = idsj.iter().cloned().collect();

    // Build the two majority configs.
    let c = MajorityConfig::new(ids_set);
    let cj = MajorityConfig::new(idsj_set);

    let make_lookuper = |idxs: &[Index], ids: &[u64], idsj: &[u64]| -> AckIndexer {
        let mut l = AckIndexer::default();
        // next to consume from idxs
        let mut p: usize = 0;
        for id in ids.iter().chain(idsj) {
            if !l.contains_key(id) && p < idxs.len() {
                l.insert(*id, idxs[p]);
                p += 1;
            }
        }

        // Zero entries are created by _ placeholders; we don't want
        // them in the lookuper because "no entry" is different from
        // "zero entry". Note that we prevent tests from specifying
        // zero commit indexes, so that there's no confusion between
        // the two concepts.
        l.retain(|_, index| index.index > 0);
        l
    };

    // verify length of voters
    let mut input = idxs.len();

    if data.cmd.as_str() == "vote" {
        input = votes.len();
    }

    let voters = JointConfig::new_joint_from_majorities(c.clone(), cj.clone())
        .ids()
        .len();

    if voters != input {
        return format!(
            "error: mismatched input (explicit or _) for voters {:?}: {:?}",
            voters, input
        );
    }

    // buffer for expected value
    let mut buf = String::new();

    match data.cmd.as_str() {
        "committed" => {
            let use_group_commit = false;

            let mut l = make_lookuper(&idxs, &ids, &idsj);

            let idx;

            // Branch based on whether this is a majority or joint quorum
            // test case.
            if joint {
                let cc = JointConfig::new_joint_from_majorities(c.clone(), cj.clone());
                buf.push_str(&cc.describe(&l));
                idx = cc.committed_index(use_group_commit, &l);
                // Interchanging the majorities shouldn't make a difference. If it does, print.
                let a_idx = JointConfig::new_joint_from_majorities(cj, c)
                    .committed_index(use_group_commit, &l);
                if a_idx != idx {
                    buf.push_str(&format!("{} <-- via symmetry\n", a_idx.0));
                }
            } else {
                idx = c.committed_index(use_group_commit, &l);
                buf.push_str(&c.describe(&l));

                // Joining a majority with the empty majority should give same result.
                let a_idx =
                    JointConfig::new_joint_from_majorities(c.clone(), MajorityConfig::default())
                        .committed_index(use_group_commit, &l);
                if a_idx != idx {
                    buf.push_str(&format!("{} <-- via zero-joint quorum\n", a_idx.0));
                }

                // Joining a majority with itself should give same result.
                let a_idx = JointConfig::new_joint_from_majorities(c.clone(), c.clone())
                    .committed_index(use_group_commit, &l);
                if a_idx != idx {
                    buf.push_str(&format!("{} <-- via self-joint quorum\n", a_idx.0));
                }

                // test overlaying
                // If the committed index was definitely above the currently inspected idx,
                // the result shouldn't change if we lower it further.
                for &id in c.ids() {
                    if let Some(iidx) = l.acked_index(id) {
                        if idx.0 > iidx.index {
                            // try index - 1
                            l.insert(
                                id,
                                Index {
                                    index: iidx.index - 1,
                                    group_id: iidx.group_id,
                                },
                            );

                            let a_idx = c.committed_index(use_group_commit, &l);
                            if a_idx != idx {
                                buf.push_str(&format!(
                                    "{} <-- overlaying {}->{}\n",
                                    a_idx.0,
                                    id,
                                    iidx.index - 1
                                ));
                            }
                            // try 0
                            l.insert(
                                id,
                                Index {
                                    index: 0,
                                    group_id: iidx.group_id,
                                },
                            );

                            let a_idx = c.committed_index(use_group_commit, &l);
                            if a_idx != idx {
                                buf.push_str(&format!(
                                    "{} <-- overlaying {}->{}\n",
                                    a_idx.0, id, 0
                                ));
                            }
                            // recovery
                            l.insert(id, iidx);
                        }
                    }
                }
            }
            buf.push_str(&format!(
                "{}\n",
                Index {
                    index: idx.0,
                    group_id: 0
                }
            ));
        }
        "vote" => {
            let ll = make_lookuper(&votes, &ids, &idsj);
            let mut l = HashMap::default();
            for (id, v) in ll {
                l.insert(id, v.index != 1);
            }

            let r;
            if joint {
                // Run a joint quorum test case.
                r = JointConfig::new_joint_from_majorities(c.clone(), cj.clone())
                    .vote_result(|id| l.get(&id).cloned());
                // Interchanging the majorities shouldn't make a difference. If it does, print.
                let ar = JointConfig::new_joint_from_majorities(cj, c)
                    .vote_result(|id| l.get(&id).cloned());
                if ar != r {
                    buf.push_str(&format!("{} <-- via symmetry\n", ar));
                }
            } else {
                r = c.vote_result(|id| l.get(&id).cloned());
            }
            buf.push_str(&format!("{}\n", r));
        }
        _ => {
            panic!("unknown command: {}", data.cmd);
        }
    }
    buf
}

#[test]
fn test_data_driven_quorum() -> anyhow::Result<()> {
    let logger = default_logger();
    run_test("src/quorum/testdata", test_quorum, false, &logger)?;
    Ok(())
}

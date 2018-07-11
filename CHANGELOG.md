# 0.3.0 - 2018-06-07

- Support configuring the election timeout range (https://github.com/pingcap/raft-rs/pull/63).
- Keep compatible with rust-protobuf 2.0 (https://github.com/pingcap/raft-rs/pull/64, https://github.com/pingcap/raft-rs/pull/75)
- Made Raft now `Send` (https://github.com/pingcap/raft-rs/pull/67)
- Added documentation examples (https://github.com/pingcap/raft-rs/pull/69)
- Fixed a deadlock in the prevote migration process (https://github.com/pingcap/raft-rs/pull/42)

# 0.2.0 - 2018-05-14

- Deprecate sync-log and add context (https://github.com/pingcap/raft-rs/pull/59)
- Fix learner isolation bug (https://github.com/pingcap/raft-rs/pull/58)
- Port several tests (https://github.com/pingcap/raft-rs/pull/54, https://github.com/pingcap/raft-rs/pull/41)
- Add examples (https://github.com/pingcap/raft-rs/pull/44)
- Use fxhash (https://github.com/pingcap/raft-rs/pull/48)

# 0.1.0 - 2018-02-26

- initial release

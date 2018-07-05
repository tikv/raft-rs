# 0.3.0 - 2018-06-07

- Now supports configuring the election timeout range (https://github.com/pingcap/raft-rs/pull/63).
- Fixed a compiler error (https://github.com/pingcap/raft-rs/pull/64)
- Made Raft now `Send` (https://github.com/pingcap/raft-rs/pull/67)
- Added documentation examples (https://github.com/pingcap/raft-rs/pull/69)
- Workaround a problem with Protobufs (https://github.com/pingcap/raft-rs/pull/75)
- Fixed a deadlock in the prevote migration process (https://github.com/pingcap/raft-rs/pull/42)

# 0.2.0 - 2018-05-14

- Deprecated sync-log and add context (https://github.com/pingcap/raft-rs/pull/59)
- Fix learner isolation bug (https://github.com/pingcap/raft-rs/pull/58)
- Port several tests (https://github.com/pingcap/raft-rs/pull/54, https://github.com/pingcap/raft-rs/pull/41)
- Add examples (https://github.com/pingcap/raft-rs/pull/44)
- Use fxhash (https://github.com/pingcap/raft-rs/pull/48)

# 0.1.0 - 2018-02-26

- initial release

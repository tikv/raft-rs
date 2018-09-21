# 0.4.0 - 2018-09-18

- No longer scan the raft log when becoming a leader. (https://github.com/pingcap/raft-rs/pull/100)
- Added the ability to skip broadcast commit at runtime. (https://github.com/pingcap/raft-rs/pull/115)
- Documented all public API. (https://github.com/pingcap/raft-rs/pull/87)
- Refined a few points in the API in preparation for more work. (https://github.com/pingcap/raft-rs/pull/102)
- Configuration logic was moved into its own module. (https://github.com/pingcap/raft-rs/pull/91)
- Added [`fail-rs`](https://github.com/pingcap/fail-rs) based tests. (https://github.com/pingcap/raft-rs/pull/114)
- Added benchmarking using [`criterion`](). (https://github.com/pingcap/raft-rs/pull/110)
- Expanded tested examples. (https://github.com/pingcap/raft-rs/pull/118)
- Improved documentation. (https://github.com/pingcap/raft-rs/pull/106)
- Refined the CI scripts to ensure strict linting. (https://github.com/pingcap/raft-rs/pull/117)
- Tests now output logs. Configure it with `RUST_LOG=raft=info`. (https://github.com/pingcap/raft-rs/pull/103)
- Eased the `log` dependency. (https://github.com/pingcap/raft-rs/pull/116)
- Formatting updates. (https://github.com/pingcap/raft-rs/pull/104)
- Updated some dependencies. (https://github.com/pingcap/raft-rs/pull/97)
- Use the clippy preview from Rustup. (https://github.com/pingcap/raft-rs/pull/95)
- Adopted a Code of Conduct. (https://github.com/pingcap/raft-rs/pull/107)

# 0.3.1 - 2018-07-10

- Bugfix: Reset leader_id when becoming precandidate to resolve `prevote` and `check_quorum` compatability (https://github.com/pingcap/raft-rs/pull/84)
- Bugfix: Becoming a precandidate should reset votes (https://github.com/pingcap/raft-rs/pull/83)
- Fixed some typos, improve variable naming, and other small documentation fixes (https://github.com/pingcap/raft-rs/pull/77, https://github.com/pingcap/raft-rs/pull/79, https://github.com/pingcap/raft-rs/pull/78, https://github.com/pingcap/raft-rs/pull/80)
- Implemented `Default` for `Config` and fleshed out an example (https://github.com/pingcap/raft-rs/pull/81)
- Improved our changelog format (https://github.com/pingcap/raft-rs/pull/85)
- Removed custom Rustfmt configuration (https://github.com/pingcap/raft-rs/pull/86)

# 0.3.0 - 2018-06-07

- Supported configuring the election timeout range (https://github.com/pingcap/raft-rs/pull/63).
- Kept compatible with rust-protobuf 2.0 (https://github.com/pingcap/raft-rs/pull/64, https://github.com/pingcap/raft-rs/pull/75)
- Made Raft now `Send` (https://github.com/pingcap/raft-rs/pull/67)
- Added documentation examples (https://github.com/pingcap/raft-rs/pull/69)
- Fixed a deadlock in the prevote migration process (https://github.com/pingcap/raft-rs/pull/42)

# 0.2.0 - 2018-05-14

- Deprecated sync-log and add context (https://github.com/pingcap/raft-rs/pull/59)
- Fixed learner isolation bug (https://github.com/pingcap/raft-rs/pull/58)
- Ported several tests (https://github.com/pingcap/raft-rs/pull/54, https://github.com/pingcap/raft-rs/pull/41)
- Added examples (https://github.com/pingcap/raft-rs/pull/44)
- Added fxhash (https://github.com/pingcap/raft-rs/pull/48)

# 0.1.0 - 2018-02-26

- initial release

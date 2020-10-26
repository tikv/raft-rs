# Overview

**A bit of history:** The Raft-rs library started life inside TiKV, a (now CNCF) project to develop a distributed database. It has existed in some prototype form since Rust around 0.7, and has gone through many changes and evolutions. At the start of 2018 PingCAP packaged this Raft implementation for others to use under the Apache 2.0 license.

Raft, as a distributed consensus algorithm, occupies a core part of the systems that utilize it. While we do not expect contributors to be experts on the algorithm, or Rust, please be aware that the review process for Raft may be longer and more strict than some other projects you have contributed to in the past.

**We'd love it** if you used Raft and reported (or even fixed!) any problems you find, whether it be bugs, safety flaws, or even usability issues. You are welcome to improve existing code, clean up modules, better organize tests, add in tooling and instrumentation, and help us to everything we can to make sure the project is rock solid and an absolute joy to use.

There is a lot of work to do to get there, and we're very excited for you to consider contributing.

# Process for Contributors

1. **Choose, or report, any issue you want!** Try to keep things bite sized, you may have a lot of ideas, but try to limit your changes to ease future review. If your resulting pull request is too big we will ask you to split it up, and that's no fun!
    * [These are good first issues.](https://github.com/pingcap/raft-rs/labels/Good%20First%20Issue)
	* [These are issues we'd love to complete with your help.](https://github.com/pingcap/raft-rs/labels/Help%20Wanted)
	* Many issues are "big ideas" which you are welcome to break down and partially complete.
2. **Tell us you want to tackle it.** If you tell us you're working on something we are very happy to mentor you and pair with you on problems if you want. We'll try to occasionally check in on your progress and see if you need support. Doing this also helps avoid two people doing the same thing.
3. **Tackle it!** Try to break up your work into a [story](https://about.futurelearn.com/blog/telling-stories-with-your-git-history).
	* Once you've opened a PR and requested reviews, try to avoid rebasing (please merge instead).
	* If you add any API surface, please test it.
	* If you add a feature, tests are required.
	* Benchmarks are highly encouraged.
	* Try to avoid panicking, return `Result<T>` instead. If you return a result, please test all possible paths do not mutate the `Raft` on failure.
	* Avoid public fields, use getters and setters. In the future changing a field could break an API and force a major version update.
4. **Prepare it.** Groom your code before you send a PR. The CI will verify you did all of this.
	* Run `cargo test --all`, make sure they all pass.
	* Run `cargo bench --all -- --test`, make sure they all pass.
	* Run `cargo clippy --all --all-targets -- -D clippy::all`, fix any lints.
	* Run `cargo fmt --all`.
5. **Submit the Pull Request.**
	* Make sure to link to the issue you are addressing. If there is no related issue, please describe what the PR resolves.
	* If you're still working on things, or you aren't ready for a full review yet, you can put `WIP:` in the title. You can use this as a chance to ask for some feedback or help.
	* Look at the pending changes, give it a quick browse. Use this as a chance to check for `TODOs` or newly incorrect documentation.
	* Someone will review your code and assign another reviewer.
	* Discuss and consider reflecting any feedback. If you choose to abandon the PR at this point we may choose to drive it to completion, you can let us know if you don't have more time.
6. Once you have two approving reviews: **Your PR is merged.**

# Process for Reviewers

1. **Find a Pull Request** requiring review, or be requested to review one.
2. **Review the description.**. If the description does not clearly state the problem it resolves, ask the author to change it so it does.
3. **Thoroughly review the code.** Take your time, consider the context and possible corner cases. Anywhere which causes confusion you should request some explanation. If you think it's better as documentation in the code please specify that.
4. If you approve of the change, use **use the 'Submit Review' button to give approval.**
5. **Request a second reviewer,** if you think it's ready. If you gave a lot of change requests, or think the PR is in the wrong direction, you can wait to do this.
6. If you're the second reviewer, **review it** and if it's good, leave an approval then **merge it**.
6. If the PR is by a contributor, make sure we **thank them for their efforts**.

# Becoming a Maintainer

If you contribute a major feature, or help us tackle several bugs, we may invite you to become a maintainer. As a maintainer we will work with you in any project planning. As you will help reduce our maintenance responsibility, it will be our new responsibility ensure you have a voice in the project.

You'll also get cool swag, and any time we're in your area we'll do our best to treat you to a meal and/or drinks. :)

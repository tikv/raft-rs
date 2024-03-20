# The TLA+ Specification for Raft-RS

## Overview

Welcome to the TLA+ specification for Raft-RS!

The TLA+ specification is tailored specifically for Raft-RS.
Unlike existing TLA+ specifications for the Raft protocol, which tend to be high-level and abstract,
we write specifications closely with the codebase.
By doing so, we aim to capture the design choices and optimizations made in the implementation,
thereby enabling model checking the implementation core logic to uncover subtle bugs and edge cases at the specification level.

At present, the specification modeled the basic Raft modules, including leader election and log replication.
Other modules (e.g., log compaction and membership change) are planned to model.
The specification assumes the UDP failure model because Raft-RS is designed to be agnostic of the underlying transport layer.
The UDP failure model allows message drop, duplication, and unordered delivery.

We have conducted certain scale of model checking to verify the correctness of the specification. 
We tested the model for 24 hours and found 344,103,609 states with no statute violations.
The specification can serve as the super-doc supplementing detailed system documentation for the Raft-RS developers.

If you have any question or find any problem of the specification, please contact us.


## Running the model checker
The specifications in this folder are implemented for and were checked with the TLC model checker, specifically with the nightly build of TLC. The scripts in this folder allow you to run TLC using the CLI easily.

To download and then run TLC, simply execute:

```shell
cd raft-rs-tla
make download
make run args="consensus/WrapperTcp.tla"
```

## Tip 
TLC works best if it can utilize all system resources. You can find the parameter settings for the worker in the run field of the makefile and adjust them to suit your computer's parameters

---- MODULE WrapperTcp ----
EXTENDS RaftRs, TLC

spec ==
Spec
----

inv_TypeOk ==
TypeOk
inv_INV ==
INV
----

CONSTANTS
v1, v2
const_Commands ==
{v1, v2}
----

CONSTANTS
n1, n2, n3
const_Servers ==
{n1, n2, n3}
----

const_Parameters == 
[MaxElectionTimes |-> 4,
MaxRestart 				  |-> 1,
MaxAppendEntriesTimes        |-> 4,
MaxHeartBeatTimes            |-> 4,
MaxPartitionTimes            |-> 0,
MaxClientOperationsTimes     |-> 3,
MaxWireMsgs                  |-> 8]
----

symm_2 ==
Permutations(const_Commands) \union Permutations(const_Servers)
----

constr_SC ==
SC
====================

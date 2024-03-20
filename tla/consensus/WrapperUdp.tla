---- MODULE WrapperUdp ----
EXTENDS RaftRsUdp, TLC

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
[ MaxElectionTimes |-> 3,
MaxRestart 				  |-> 1,
MaxAppendEntriesTimes        |-> 4,
MaxDupTimes                  |-> 1,
MaxDropTimes                 |-> 1,
MaxHeartBeatTimes            |-> 3,
MaxUnorderTimes              |-> 4,
MaxClientOperationsTimes     |-> 6,
MaxWireMsgs                  |-> 8 ]
----

symm_2 ==
Permutations(const_Commands) \union Permutations(const_Servers)
----

constr_SC ==
SC
====================

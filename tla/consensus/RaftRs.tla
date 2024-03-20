----------------------------  MODULE RaftRs ----------------------------- 
(***************************************************************************)
(* This is the TLA+ specification for Raft-RS in TiKV with version 0.7.0   *)
(*                                                                         *)
(* - Leader election:                                                      *)
(* - Log replication:                                                      *)
(*                                                                         *)
(* Currently, the specification assumes:                                   *)
(* - No snapshots                                                          *)
(* - No read-only requests                                                 *)
(* - No non-voting nodes                                                   *)
(* - No disk failures                                                      *)
(* - No membership change                                                  *)
(***************************************************************************)

EXTENDS Sequences, Naturals, Integers, FiniteSets, TLC, SequencesExt

(***************************************************************************)
(* Constants definitions                                                   *)
(***************************************************************************)
\* The set of servers
CONSTANT Servers
\* Server states, Corresponding to raft-rs StateRole
CONSTANTS Follower, Candidate, Leader 
\* Raft message types
CONSTANTS M_RV, M_RVR, M_AE, M_AER, M_PRV, M_PRVR, M_HB, M_HBR
\* The set of commands
CONSTANTS Commands 
\* The abstraction of null operation
CONSTANTS NoOp
\* Misc: state constraint parameters and placeholder
CONSTANTS Nil  
\* The set of ProgressState
CONSTANTS Probe, Replicate

(***************************************************************************
  Variables definitions
 ***************************************************************************)
\* Persistent state on all servers
VARIABLES currentTerm,                 \* Latest term server has seen (initialized to 0 on first boot, increases monotonically) , Corresponding to raft-rs RaftCore.term
          votedFor,                    \* CandidateId that received vote in current term (or null if none), Corresponding to raft-rs RaftCore.vote
          log                          \* Log entries; each entry contains command for state machine, and term when entry was received by leader, Corresponding to raft-rs RaftCore.raft_log


\* Volatile state on all servers
VARIABLES raftState,                   \* State of servers, in {Follower, Candidate, Leader} , Corresponding to raft-rs RaftCore.state
          commitIndex,                 \* Index of highest log entry known to be committed
          leader_id                    \* The potential leader of the cluster, Corresponding to raft-rs RaftCore.leader_id


\* Volatile state on leader     
VARIABLES nextIndex,                   \* for each server, index of the next log entry to send to that  server, Corresponding to raft-rs Progress.next_idx
          matchIndex                   \* for each server, index of highest log entry known to be replicated on server, Corresponding to raft-rs Progress.matched

\* intermediate variable
VARIABLES voted_for_me   \* Record nodes that have voted for me, Corresponding to raft-rs Progress.voted
VARIABLES voted_reject   \* Record nodes that have not voted for me, Corresponding to raft-rs Progress.voted
VARIABLES check_quorum   \* check_quorum variables
VARIABLE progress        \* The status of each follower's receive log, which is used in receiving append, which contains probe and replicate. Corresponding to raft-rs Progress.state
VARIABLE inflight        \* Number of letters transmitted during the recording process. Corresponding to raft-rs Progress.int (Inflights)  



(***************************************************************************
  Network variables and instance
 ***************************************************************************)
\* The network is modelled through these three variables
VARIABLES netman, 
          netcmd,
          msgs
INSTANCE FifoNetwork WITH FLUSH_DISCONN <- TRUE, NULL_MSG <- Nil,
    _msgs <- msgs, _netman <- netman, _netcmd <- netcmd


(***************************************************************************)
(* Self manipulated invariants checking                                    *)
(***************************************************************************)
VARIABLES inv \* Invariants that guarantee correctness

(***************************************************************************)
(* Vars groups                                                             *)
(***************************************************************************)
serverVars    == <<currentTerm, votedFor, raftState>>
leaderVars    == <<nextIndex, matchIndex>>
candidateVars == <<voted_for_me, voted_reject>>
logVars       == <<log, commitIndex>>
nodeVars      == <<leader_id, check_quorum, progress, inflight>>
netVars       == <<netman, netcmd, msgs>>
noNetVars     == <<serverVars, leaderVars, candidateVars, logVars, nodeVars>>
vars          == <<noNetVars, netVars, inv>>


(***************************************************************************)
(* State constraints helper                                                *)
(***************************************************************************)
CONSTANTS Parameters  \* to control the model scale

GetParameterSet(p)  == IF p \in DOMAIN Parameters THEN Parameters[p] ELSE {}

CheckParameterHelper(n, p, Test(_,_)) ==
    IF p \in DOMAIN Parameters
    THEN Test(n, Parameters[p])
    ELSE TRUE
CheckParameterMax(n, p) == CheckParameterHelper(n, p, LAMBDA i, j: i <= j)

PrePrune(n, p) == CheckParameterHelper(n, p, LAMBDA i, j: i < j)


(***************************************************************************)
(* Type Ok. Used as a check on writing format                              *)
(***************************************************************************)

TypeOkServerVars == 
    /\ currentTerm \in [ Servers -> Nat ]
    /\ votedFor    \in [ Servers -> Servers \cup {Nil} ]
    /\ raftState   \in [ Servers -> { Follower, Candidate, Leader } ] 

TypeOkLeaderVars ==
    /\ nextIndex   \in [ Servers -> [ Servers -> Nat \ {0} ]]
    /\ matchIndex  \in [ Servers -> [ Servers -> Nat ]]

\* TypeOkCandidateVars ==
\*     /\ votesGranted  \in [ Servers -> {} ]

TypeOkLogVars ==
    \* log data structure is complex, we skip checking it
    /\ commitIndex \in [ Servers -> Nat ]

TypeOk ==
    /\ TypeOkServerVars
    /\ TypeOkLeaderVars
    /\ TypeOkLogVars


(***************************************************************************
  Init variables
 ***************************************************************************)
InitServerVars ==
    /\ currentTerm = [ i \in Servers |-> 1 ]
    /\ votedFor    = [ i \in Servers |-> Nil ]
    /\ raftState   = [ i \in Servers |-> Follower ]

InitLeaderVars ==
    /\ nextIndex  = [ i \in Servers |-> [ j \in Servers |-> 1 ]]
    /\ matchIndex = [ i \in Servers |-> [ j \in Servers |-> 0 ]]

InitCandidateVars ==
    /\ voted_for_me = [ i \in Servers |-> {} ]
    /\ voted_reject = [ i \in Servers |-> {} ]

InitLogVars ==
    /\ log = [ i \in Servers |-> << [term |-> 1, data |-> Nil, index |-> 1]>> ]
    /\ commitIndex = [ i \in Servers |-> 1 ]
InitInv == inv = <<>>

InitNodeVars == 
    /\ leader_id = [ i \in Servers |-> Nil]
    /\ check_quorum = [i \in Servers |-> FALSE]  \* Used to determine if check_quorum is on
    /\ progress = [ i \in Servers |-> [ j \in Servers |-> <<Probe, FALSE>>]] 
    /\ inflight = [ i \in Servers |-> [ j \in Servers |-> 0 ]]

InitNetVars ==
    /\ InitFifoNetworkAddNetman(Servers, <<"Init", Cardinality(Servers)>>, 
                [n_elec |-> 0, n_ae |-> 0, n_hb |-> 0, n_op |-> 0, n_restart |-> 0,
                 no_inv |-> GetParameterSet("NoInv")])


Init ==
    /\ InitServerVars
    /\ InitLeaderVars
    /\ InitCandidateVars
    /\ InitLogVars
    /\ InitInv
    /\ InitNodeVars
    /\ InitNetVars

(***************************************************************************
  Helper functions
 ***************************************************************************)
NumServer == Cardinality(Servers)

Min(x,y) == IF x < y THEN x ELSE y
Max(x,y) == IF x < y THEN y ELSE x

IsQuorum(ss) == Cardinality(ss) * 2 > Cardinality(Servers)
IsQuorumNum(num) == num * 2 > Cardinality(Servers)

CheckStateIs(n, s)   == raftState[n] = s
CheckStateIsNot(n, s) == raftState[n] /= s

Update(var, n, value) == [var EXCEPT ![n] = value]
UpdateCurrentTerm(n, term) == currentTerm' = Update(currentTerm, n, term)
UpdateLeaderId(n, id) == leader_id' = Update(leader_id, n, id)
UpdateVotedFor(n, node) == votedFor' = Update(votedFor, n, node)
UpdateState(n, s) == raftState' = Update(raftState, n, s)
UpdateVotedForMe(n, value) == voted_for_me' = Update(voted_for_me, n, value)
AddVotedForMe(me, node) == voted_for_me' = [ voted_for_me EXCEPT ![me] = @ \cup {node} ]
ClearVotedForMe(me) == voted_for_me' = [ voted_for_me EXCEPT ![me] = {} ]
UpdateVotesReject(n, value) == voted_reject' = Update(voted_reject, n, value)
AddVotesReject(me, node) == voted_reject' = [ voted_reject EXCEPT ![me] = @ \cup {node}]
ClearVotesReject(me) == voted_reject' = [ voted_reject EXCEPT ![me] = {} ]
UpdateMatchIdx(me, node, idx) == matchIndex' = [ matchIndex EXCEPT ![me][node] = idx ]
UpdateNextIdx(me, node, idx) == nextIndex' = [ nextIndex EXCEPT ![me][node] = IF idx < 1 THEN 1 ELSE idx ]
UpdateProgress(me, node, state) == progress' = [progress EXCEPT ![me][node] = state ]
UpdateInflight(me, node, num) == inflight' = [inflight EXCEPT ![me][node] = num ]
UpdateCommitIdx(n, idx) == commitIndex' = Update(commitIndex, n, idx)
AllUpdateNextIdx(me, idx) ==
    LET f == [i \in Servers |-> idx]
    IN  nextIndex' = [nextIndex EXCEPT ![me] = f]
AllUpdateMatchIdx(me, idx) == 
    LET f == [i \in Servers |-> idx]
    IN  matchIndex' = [matchIndex EXCEPT ![me] = f]
AllUpdateProgress(me, prstate) == 
    LET f == [i \in Servers |-> prstate]
    IN  progress' = [progress EXCEPT ![me] = f]
AllUpdateInflight(me, num_msg) == 
    LET f == [i \in Servers |-> num_msg]
    IN  inflight' = [inflight EXCEPT ![me] = f]

(***************************************************************************)
(* Log helpers                                                             *)
(***************************************************************************)
\* Currently, the log won't be compacted
\* idx = 1, data = Nil  
LogAppend(log_, entry) == Append(log_, entry)
LogCount(log_) == Len(log_)
LogGetEntry(log_, idx) ==
    IF idx > LogCount(log_) \/ idx <= 0 
    THEN Nil 
    ELSE log_[idx]
LogGetEntryOne(log_, idx) ==
    IF idx > LogCount(log_) \/ idx <= 0 
    THEN <<>>
    ELSE SubSeq(log_, idx, idx)
LogGetEntriesFrom(log_, idx) ==
    IF idx > LogCount(log_) \/ idx <= 0 THEN <<>>
    ELSE SubSeq(log_, idx, LogCount(log_))
LogGetEntriesTo(log_, idx) ==
    IF Len(log_) < idx THEN log_
    ELSE SubSeq(log_, 1, idx)
LogDeleteEntriesFrom(log_, idx) == SubSeq(log_, 1, idx - 1)
LogCurrentIdx(log_) == LogCount(log_)
LogLastTerm(log_) ==
    LET idx == LogCount(log_)
        term == IF idx = 0 THEN 0 ELSE log_[idx].term
    IN term
LogLastIdx(log_) ==
    LET idx == LogCount(log_)
        index == IF idx = 0 THEN 0 ELSE log_[idx].index
    IN index
LogGetTerm(log_, idx, info) ==
    IF LogCount(log_) < idx
    THEN 0
    ELSE IF idx = 0 THEN 0 ELSE log_[idx].term

\* log_ is the log of the original node, entries is the logs that need to be added in the AE letter, we need to find a suitable location to overwrite the conflicting logs according to the incoming prevLogIdx, and add the subsequent logs.  
\* in maybe_append@raft_log.rs 
LogGetMatchEntries(log_, entries, prevLogIdx) ==
    LET F[i \in 0..Len(entries)] ==
            IF i = 0 THEN Nil
            ELSE LET ety1 == LogGetEntry(log_, prevLogIdx + i) \* Original log Entry at prevLogIdx + i
                     ety2 == LogGetEntry(entries, i) \* The entries ith one to be added
                     entries1 == LogGetEntriesTo(log_, prevLogIdx + i - 1)  \* log_ from first_index to prevLogIdx + i - 1
                     entries2 == LogGetEntriesFrom(entries, i) \* entries from i to Len(entries)
                 IN IF /\ F[i-1] = Nil
                       /\ \/ ety1 = Nil  \* The original log does not have the ith one, indicating that all subsequent ones need to be added directly.
                          \/ ety1.term /= ety2.term \* The i-th mismatch of the original log indicates that it needs to be overwritten from the i-th onwards with all newly added
                    THEN entries1 \o entries2
                    ELSE F[i-1]
        result == F[Len(entries)]
    IN IF result = Nil THEN log_ ELSE result  


(***************************************************************************)
(* Msg constructors                                                        *)
(***************************************************************************)
\* Send the letter to the remaining nodes, constructing the letter according to the rules of the Contrustor2/3 function
_BatchExcludesReqMsgsArg(n, excludes, Constructor2(_, _), Constructor3(_, _, _), arg) ==
    LET dsts == Servers \ excludes
        size == Cardinality(dsts)
        F[i \in 0..size] ==
            IF i = 0 THEN <<<<>>, dsts>>
            ELSE LET ms == F[i-1][1]
                     s == CHOOSE j \in F[i-1][2]: TRUE
                     m == IF arg = Nil
                          THEN Constructor2(n, s)
                          ELSE Constructor3(n, s, arg)
                     remaining == F[i-1][2] \ {s}
                 IN <<Append(ms, m), remaining>>
    IN F[size][1]

_Dummy2(a, b) == TRUE
_Dummy3(a, b, c) == TRUE

BatchReqMsgs(n, Constructor(_, _)) ==
    _BatchExcludesReqMsgsArg(n, {n}, Constructor, _Dummy3, Nil)
BatchReqMsgsArg(n, Constructor(_, _, _), arg) ==
    _BatchExcludesReqMsgsArg(n, {n}, _Dummy2, Constructor, arg)
ConstructMsg(src, dst, type, body) ==
    [ src |-> src, dst |-> dst, type |-> type, data |-> body ]

\* func：new_message(MsgRequestVote)@raft.rs  
RequestVote(i, j) ==  
    LET body == [ term |-> currentTerm'[i],
                  candidate_id |-> i,
                  index |-> LogCurrentIdx(log[i]),
                  log_term |-> LogLastTerm(log[i]),
                  commit |-> commitIndex[i],
                  commitTerm |-> LogGetTerm(log[i], commitIndex[i], "RequestVote")]
        msg_type ==  M_RV
    IN ConstructMsg(i, j, msg_type, body)

\* func：new_message(MsgRequestVoteResponse)@raft.rs   
RequestVoteResponse(m, voted, tempLeaderId) ==  
    LET i == m.dst
        j == m.src
        req == m.data    
        \* can_vote corresponding to step()@raft.rs, which define the situation it can vote or not
        can_vote == \/ voted = j
                    \/ /\ voted = Nil
                       /\ tempLeaderId = Nil 
        meTerm == currentTerm'[i]
        rejectMeTermIsBigger == meTerm > req.term
        meLastTerm == LogLastTerm(log[i])
        rejectMeLogNewer == \/ req.log_term < meLastTerm
                            \/ /\ req.log_term = meLastTerm
                               /\ req.index < LogCurrentIdx(log[i])        
        voteStatus == IF rejectMeTermIsBigger THEN "not-vote: term bigger"   ELSE
                      IF ~can_vote            THEN "not-vote: can not vote" ELSE
                      IF rejectMeLogNewer     THEN "not-vote: log newer"     ELSE "voted"
        granted == voteStatus = "voted"
        reject == ~granted
        send_commit == IF reject THEN commitIndex[i] ELSE 0
        send_commit_term == IF reject THEN LogGetTerm(log[i], commitIndex[i], "RequestVoteResponse") ELSE 0
        body == [ request_term |-> req.term,
                  term |-> Max(req.term, meTerm),
                  reject |-> reject,
                  commit |-> send_commit,
                  commitTerm |-> send_commit_term]
    IN ConstructMsg(i, j, M_RVR, body) @@ [ status |-> voteStatus ]

\* func: prepare_send_entries 
AppendEntriesNext(i, j, next) ==      
    LET prev_log_idx == next[i][j] - 1
        body == [ term |-> currentTerm[i],
                  leader_id |-> i,
                  commit |-> commitIndex'[i],
                  index |->  prev_log_idx,  \* prev_log_idx
                  log_term |-> IF LogCount(log'[i]) >= prev_log_idx 
                               THEN LogGetTerm(log'[i], prev_log_idx, "AppendEntriesNext") 
                               ELSE 0 ,
                  entries |-> LogGetEntryOne(log'[i], next[i][j]) ]  \* The model restricts AppendEntry messages to one entry at a time.
    IN ConstructMsg(i, j, M_AE, body)

\* func: send_heartbeat 
HeartBeatNext(i, j, next) ==  
    LET body == [ term |-> currentTerm[i],
                  commit |-> Min(matchIndex[i][j], commitIndex[i])]
    IN ConstructMsg(i, j, M_HB, body)

HeartBeatResponse(m) ==
    LET body == [ term |-> currentTerm'[m.dst],
                  commitIdx |-> commitIndex'[m.dst] ]
    IN ConstructMsg(m.dst, m.src, M_HBR, body)

\* new_message(MsgAppendResponse)@raft.rs  
AERFailLogStale(m) ==  \* func: handle_append_entries
    LET body == [ reject |-> FALSE,
                  term |-> Max(currentTerm[m.dst], m.data.term),
                  index |-> commitIndex[m.dst],                 
                  commit |-> commitIndex[m.dst] ]
    IN ConstructMsg(m.dst, m.src, M_AER, body)

\* new_message(MsgAppendResponse)@raft.rs  
AERFailTermMismatch(m, hint_index, hint_term) ==
    LET body == [ reject |-> TRUE,
                  term |-> Max(currentTerm[m.dst], m.data.term),  
                  index |-> m.data.index,  
                  reject_hint |-> hint_index,           
                  log_term |-> hint_term,
                  commit |-> commitIndex[m.dst] ]
    IN ConstructMsg(m.dst, m.src, M_AER, body)

\* new_message(MsgAppendResponse)@raft.rs  
AppendEntriesResponseSuccess(m) == 
    LET data == m.data
        body == [ reject |-> FALSE,
                  term |-> currentTerm'[m.dst],
                  index |-> data.index + Len(data.entries),
                  commitIdx |-> commitIndex'[m.dst]]
    IN ConstructMsg(m.dst, m.src, M_AER, body)


\* At bcast_append the next_index of the node to the target node is updated for each letter.(in prepare_send_entries@raft.rs) 
BatchUpdateNextWithMsg(n, new_msgs) ==
    LET lenMsg == Len(new_msgs)
        F[i \in 0..lenMsg] ==
            IF i = 0 THEN <<{}, Servers, (n :> 1)>>
            ELSE LET dst == new_msgs[i].dst
                     ety == new_msgs[i].data.entries
                     etyLastIdx == LogLastIdx(ety)
                IN IF \/ ety = <<>> \* If the content of the letter is empty, no need to update
                      \/ progress[n][dst][1] = Probe \* If a node is in the Probe state, sending at this point will block( maybe_send_append().is_paused() @ raft.rs)
                   THEN <<F[i-1][1] , F[i-1][2], F[i-1][3] @@ (dst :> etyLastIdx) >> 
                   ELSE <<F[i-1][1] \cup {n}, F[i-1][2] \ {n}, F[i-1][3] @@ (dst :> etyLastIdx)>>
        updateServer == F[lenMsg][1]
        remainServer == F[lenMsg][2]
        updateMap == F[lenMsg][3]
        next_keep == [ s \in remainServer |-> nextIndex[n][s] ] 
        next_update == [ s \in updateServer |-> updateMap[s] ] 
    IN nextIndex' = [ nextIndex EXCEPT ![n] = next_keep @@ next_update ]



(***************************************************************************)
(* Raft actions                                                            *)
(***************************************************************************)

\* func reset
reset(i) == 
    /\ ClearVotedForMe(i)
    /\ ClearVotesReject(i)
    /\ AllUpdateNextIdx(i, LogCount(log[i]) + 1)
    /\ AllUpdateMatchIdx(i, 0)
    /\ AllUpdateProgress(i, <<Probe, FALSE>>)
    /\ AllUpdateInflight(i, 0)

(***************************************************************************)
(* Become candidate                                                        *)
(***************************************************************************)

\* func: become_candidate
BecomeCandidate(i) ==  
    /\ UpdateCurrentTerm(i, currentTerm[i] + 1)
    /\ UpdateVotedFor(i, i)
    /\ reset(i)
    /\ UpdateLeaderId(i, Nil)
    /\ UNCHANGED << check_quorum, logVars>>
    /\ UpdateState(i, Candidate)
    /\ LET ms == BatchReqMsgs(i, RequestVote)
       IN NetUpdate2(NetmanIncField("n_elec", NetBatchAddMsg(ms)), <<"BecomeCandidate", i>>)

(***************************************************************************)
(* Become leader                                                           *)
(***************************************************************************)

\* func: become_leader@raft.rs
BecomeLeader(i, m) ==  
    /\ LET noop == [ term |-> currentTerm[i], data |-> Nil, index |-> LogCount(log[i]) + 1 ]
       IN log' = Update(log, i, LogAppend(log[i], noop))
    /\ UpdateState(i, Leader)
    /\ UpdateLeaderId(i, i)
    /\ ClearVotedForMe(i)
    /\ ClearVotesReject(i)
    /\ matchIndex' = [ matchIndex EXCEPT ![i] = ( i :> LogCurrentIdx(log'[i]) ) @@ [ j \in Servers |-> 0 ] ] 
    /\ AllUpdateProgress(i, <<Probe, TRUE>>) \* All progress needs to be in probe mode  
    /\ AllUpdateInflight(i, 0) \* All inflight needs to be 0 (no message send)
    /\ LET next == [ nextIndex EXCEPT ![i] = ( i :> matchIndex'[i][i] + 1 ) @@ [ j \in Servers |-> LogCurrentIdx(log[i]) + 1] ]
           ms == BatchReqMsgsArg(i, AppendEntriesNext, next)
       IN  /\ nextIndex' = next 
           /\ NetUpdate2(NetReplyBatchAddMsg(ms, m), <<"RecvRequestVoteResponse", "Won-BecomeLeader", i>>) \* bcast_send

(***************************************************************************)
(* Become follower                                                         *)
(***************************************************************************)

SetCurrentTerm(i, term) ==  
    /\ UpdateCurrentTerm(i, term)
    /\ UpdateVotedFor(i, Nil)

_BecomeFollower(i) ==  
    /\ UpdateState(i, Follower)
    /\ UpdateLeaderId(i, Nil)
    /\ reset(i)   

\* func : become_follower@raft.rs
BecomeFollower(i, term) ==
    /\ SetCurrentTerm(i, term)
    /\ _BecomeFollower(i)

BecomeFollowerInLost(i, term) ==
    /\ UNCHANGED <<votedFor>>
    /\ UpdateCurrentTerm(i, term)
    /\ _BecomeFollower(i)

BecomeFollowerWithLeader(i, term, leaderId) ==
    /\ SetCurrentTerm(i, term)
    /\ UpdateState(i, Follower)
    /\ UpdateLeaderId(i, leaderId)
    /\ reset(i)

(***************************************************************************)
(* Recv requestvote                                                        *)
(***************************************************************************)

\* func: maybe_commit_by_vote@raft.rs
maybe_commit_by_vote(n, commitIdx, commitTerm) ==
    IF  \/ commitIdx = 0 
        \/ commitTerm = 0
        \/ raftState'[n] = Leader
    THEN  UNCHANGED commitIndex
    ELSE IF \/ commitIdx <= commitIndex[n]
         THEN UNCHANGED commitIndex
         ELSE IF /\ commitIdx > commitIndex[n]
                 /\ commitTerm = LogGetTerm(log[n], commitIdx, "maybe_commit_by_vote")
              THEN UpdateCommitIdx(n, commitIdx)
              ELSE UNCHANGED commitIndex

HandleMsgRV(m) == 
    LET data == m.data
        dst == m.dst
        src == m.src
        demote == currentTerm[dst] < data.term
        stale == currentTerm[dst] > data.term
        msg == RequestVoteResponse(m, IF demote THEN Nil ELSE votedFor[dst], IF demote THEN Nil ELSE leader_id[dst])      \* Pass in intermediate values based on demote status.
    IN IF stale \* stale message drop
       THEN /\ UNCHANGED noNetVars 
            /\ NetUpdate2(NetDelMsg(m), 
                    <<"RecvRequestVote", "stale message ignore",  dst, src, m.seq>>)
       ELSE /\ UNCHANGED <<check_quorum>> 
            /\ IF demote  \* Received a newerletter and became a follower.
               THEN /\ UpdateCurrentTerm(dst, data.term)
                    /\ UpdateState(dst, Follower)
                    /\ UpdateLeaderId(dst, Nil)
                    /\ reset(dst)
               ELSE UNCHANGED <<currentTerm, raftState, leader_id, leaderVars, candidateVars, progress, inflight>>
            /\ IF ~msg.data.reject  \* Determine whether to vote based on RequestVote letter
               THEN /\ UpdateVotedFor(dst, src)
                    /\ UNCHANGED <<commitIndex>>
               ELSE /\ IF demote \* If there is a no vote the default is not to change the vote value, but due to the demote state, the node will reset and thus the vote will become nil
                       THEN UpdateVotedFor(dst, Nil)
                       ELSE UNCHANGED <<votedFor>> 
                    /\ maybe_commit_by_vote(dst, data.commit, data.commitTerm) \* func: maybe_commit_by_vote @ raft.rs
            /\ UNCHANGED <<log>>
            /\ NetUpdate2(NetReplyMsg(msg, m), 
                    <<"RecvRequestVote", msg.status, dst, src, m, IF ~msg.data.reject THEN "vote" ELSE "not-vote">>)


(***************************************************************************)
(* Recv requestvote response                                               *)
(***************************************************************************)

\* func : poll@raft.rs
Poll(grant, reject) ==
    LET grantNum == Cardinality(grant) + 1 \* +1 is voted for myself
        rejectNum == Cardinality(reject)
    IN IF IsQuorumNum(grantNum)
       THEN "Won"
       ELSE IF IsQuorumNum(rejectNum)
            THEN "Lost"
            ELSE "Pending"




HandleMsgRVR( m) ==
    LET resp == m.data
        src == m.src
        dst == m.dst
        demote == resp.term > currentTerm[dst] 
        isCandidate == raftState[dst] = Candidate
        stale == resp.term < currentTerm[dst] 
    IN /\ IF demote \* Received a newerletter and became a follower.
          THEN /\ UNCHANGED <<logVars, check_quorum>>
               /\ BecomeFollower(dst, resp.term)
               /\ NetUpdate2(NetDelMsg(m), <<"RecvRequestVoteResponse", "term is smaller", dst, src, m>>)
          ELSE IF  stale \* stale message drop
               THEN /\ UNCHANGED noNetVars
                    /\ NetUpdate2(NetDelMsg(m), <<"RecvRequestVoteResponse", "vote is stale", dst, src, m>>)
               ELSE IF ~isCandidate \* only candidate process M_RVR
                    THEN /\ UNCHANGED noNetVars
                         /\ NetUpdate2(NetDelMsg(m), <<"RecvRequestVoteResponse", "not candidate", dst, src, m>>)
                    ELSE    /\ UNCHANGED <<check_quorum>>
                            /\ LET newVotedForMe == IF ~resp.reject 
                                                    THEN voted_for_me[dst] \cup {src} 
                                                    ELSE voted_for_me[dst]
                                newVotedReject == IF ~resp.reject 
                                                    THEN voted_reject[dst]  
                                                    ELSE voted_reject[dst]\cup {src}
                                res == Poll(newVotedForMe, newVotedReject)
                                IN  IF res = "Won"
                                    THEN /\ UNCHANGED << commitIndex>>  \* The reason for this is that in becomeLeader we need to broadcast the AE letter globally, and the AE letter carries the latest commitIndex,  but we don't update the commitIndex until below in maybe_commit_by_vote,  and it has to use the latest commitIndex, so we need to write it here. 
                                        /\ BecomeLeader(dst, m)
                                        /\ UNCHANGED << votedFor, currentTerm>>
                                    ELSE /\ UNCHANGED <<log, inflight>>  
                                     /\ IF res = "Lost"
                                        THEN /\ BecomeFollowerInLost(dst, currentTerm[dst])
                                            /\ NetUpdate2(NetDelMsg(m), <<"RecvRequestVoteResponse", "Lost", dst, src, m>>)
                                        ELSE /\ NetUpdate2(NetDelMsg(m), <<"RecvRequestVoteResponse", "Pending", dst, src, m>>)
                                            /\ UpdateVotedForMe(dst, newVotedForMe)
                                            /\ UpdateVotesReject(dst, newVotedReject)
                                            /\ UNCHANGED << serverVars, leader_id, progress, leaderVars>>  
                            /\ maybe_commit_by_vote(dst, resp.commit, resp.commitTerm) 

(***************************************************************************)
(* Send appendentries to all other nodes                                   *)
(***************************************************************************)
SendAppendentriesAll(n) ==  \* func: bcast_append
    /\ UNCHANGED <<logVars, serverVars, matchIndex, candidateVars, nodeVars>>
    /\ LET ms == BatchReqMsgsArg(n, AppendEntriesNext, nextIndex)
       IN /\ BatchUpdateNextWithMsg(n, ms) 
          /\ NetUpdate2(NetmanIncField("n_ae", NetBatchAddMsg(ms)), <<"SendAppendentriesAll", n>>)

(***************************************************************************)
(* Send heartbeat(empty log appendentries) to all other nodes              *)
(***************************************************************************)
SendHeartBeatAll(n) ==  \* func: bcast_heart
    /\ UNCHANGED <<logVars, serverVars, leaderVars, candidateVars, nodeVars>>
    /\ LET ms == BatchReqMsgsArg(n, HeartBeatNext, nextIndex)
       IN  NetUpdate2(NetmanIncField("n_hb", NetBatchAddMsg(ms)), <<"SendHeartBeatAll", n>>)

(***************************************************************************)
(* Recv appendentries                                                      *)
(***************************************************************************)
AcceptLeader(me, leader) == 
    /\ UpdateState(me, Follower)
    /\ UpdateLeaderId(me, leader)
    /\  IF raftState[me] = Follower
        THEN UNCHANGED <<candidateVars, leaderVars, progress>>
        ELSE reset(me)

\* func: find_conflict_by_term
find_conflict_by_term(me, index, term) ==
    LET hint_index == Min(index, LogCount(log[me]))
        F[i \in 0..hint_index ] == 
            IF hint_index = 0 
            THEN <<0, 0>>
            ELSE IF i = 0
                 THEN << >>
                 ELSE IF term >= LogGetTerm(log[me] ,i, "find_conflict_by_term") 
                      THEN <<i, LogGetTerm(log[me] ,i, "find_conflict_by_term") >>
                      ELSE F[i-1]
    IN F[hint_index]

\* func: raft_log.maybe_commit()
SetCommitIdx(n, idx) ==   
    /\ Assert(idx <= LogCurrentIdx(log'[n]), <<"SetCommitIdx: idx <= LogCurrentIdx(log'[n])", n, idx, log'>>)
    /\ IF idx > commitIndex[n]
       THEN UpdateCommitIdx(n, idx)
       ELSE UNCHANGED <<commitIndex>> 

HandleMsgAE(m) ==  \* func: handle_append
    LET data == m.data
        src == m.src
        dst == m.dst
        demote == currentTerm[dst] < data.term
        stale == data.term < currentTerm[dst]
        log_stale == data.index < commitIndex[dst]
        log_stale_msg == AERFailLogStale(m)
        success == AppendEntriesResponseSuccess(m)
    IN IF stale \* drop stale message
       THEN /\ UNCHANGED noNetVars
            /\ NetUpdate2(NetDelMsg(m), <<"RecvAppendentries", "stale message ignore", dst, src, m>>)
       ELSE /\ UNCHANGED <<check_quorum, inflight>> 
            /\ IF demote \* Received a newerletter and became a follower, but there are related variables that need to be updated later, so only their term values are updated here.
               THEN SetCurrentTerm(dst, data.term)
               ELSE UNCHANGED <<currentTerm, votedFor>>
            /\ AcceptLeader(dst, data.leader_id) \* Update the leader_id and make sure the node state is follower
            /\ IF log_stale \* if m.index < self.raft_log.committed @ raft.rs
               THEN /\ UNCHANGED <<logVars>>
                    /\ NetUpdate2(NetReplyMsg(log_stale_msg, m), <<"RecvAppendentries", "log stale commit", dst, src, m>>)
               ELSE LET ety == LogGetEntry(log[dst], data.index)
                        noPrevLog == ety = Nil
                        termMatch == \/ /\ noPrevLog
                                        /\ data.log_term = 0 
                                     \/ /\ ~noPrevLog
                                        /\ ety.term = data.log_term
                    IN  IF termMatch \* maybe_append@raft_log.rs
                        THEN    /\ log' = Update(log, dst, LogGetMatchEntries(log[dst], data.entries, data.index))
                                /\  IF commitIndex[dst] < data.commit
                                    THEN LET lastLogIdx == Max(LogCurrentIdx(log'[dst]), 1)
                                             idxToCommit == Min(lastLogIdx, data.commit)
                                         IN SetCommitIdx(dst, idxToCommit)
                                    ELSE UNCHANGED commitIndex
                                /\ NetUpdate2(NetReplyMsg(success, m), <<"RecvAppendentries", "success", dst, src, m>>)
                        ELSE LET conflict == find_conflict_by_term(dst, data.index, data.log_term) \* find_conflict_by_term @ raft_log.rs
                                fail == AERFailTermMismatch(m, conflict[1], conflict[2])
                             IN  /\ UNCHANGED <<logVars>>
                                 /\ NetUpdate2(NetReplyMsg(fail, m), <<"RecvAppendentries", "term Mismatch", dst, src, m>>)


(***************************************************************************)
(* Recv appendentries response                                             *)
(***************************************************************************)            
FlushSendAppendentries(me, m, tempNextIdx, tempInflight, info) == 
    LET F[i \in 0..NumServer] ==
        IF i = 0 THEN <<{}, Servers>>
        ELSE LET n == CHOOSE n \in F[i-1][2]: TRUE
                 idx == LogCurrentIdx(log'[me])
             IN    IF n = me
                   THEN <<F[i-1][1] \cup {n}, F[i-1][2] \ {n}>>
                   ELSE IF progress'[me][n][1] = Probe
                        THEN IF progress'[me][n][2] = TRUE
                             THEN <<F[i-1][1] \cup {n}, F[i-1][2] \ {n}>>
                             ELSE <<F[i-1][1], F[i-1][2] \ {n}>>
                        ELSE IF tempInflight[me][n] /= 0 
                             THEN <<F[i-1][1] \cup {n}, F[i-1][2] \ {n}>>
                             ELSE <<F[i-1][1], F[i-1][2] \ {n}>>
        excludes == F[NumServer][1]
        excludes2 == F[NumServer][1] \ {me}
        ms == _BatchExcludesReqMsgsArg(me, excludes, _Dummy2, AppendEntriesNext, tempNextIdx)
        next_keep == [ s \in excludes2 |-> tempNextIdx[me][s] ]
        next_me == IF tempNextIdx[me][me] < LogCount(log'[me]) + 1  
                   THEN (me :> LogCount(log'[me]) + 1)
                   ELSE (me :>  tempNextIdx[me][me] )
        next_update == [ s \in Servers \ excludes |-> IF tempNextIdx[me][s] <= LogCount(log'[me])
                                                      THEN tempNextIdx[me][s] + 1
                                                      ELSE tempNextIdx[me][s]  ] 
        inflight_keep == [ s \in excludes |-> tempInflight[me][s]]
        inflight_update == [ s \in Servers \ excludes |-> IF tempNextIdx[me][s] <= LogCount(log'[me])
                                                      THEN tempNextIdx[me][s] 
                                                      ELSE 0] 
    IN /\ nextIndex' = [ nextIndex EXCEPT ![me] = next_keep @@ next_update @@ next_me]
       /\ inflight' = [inflight EXCEPT ![me] = inflight_keep @@ inflight_update]
       /\ IF m = Nil  \* RecvEntry: client request
          THEN NetUpdate2(NetmanIncField("n_op", NetBatchAddMsg(ms)), info)
          ELSE NetUpdate2(NetReplyBatchAddMsg(ms, m), info)

\* (maybe_update + maybe_commit) in handle_append_response@raft.rs
AdvanceCommitIdx(me, m, succ_rsp, tempNextIndex, tempInflight) ==  \* func: raft_update_commitIndex  
    LET F[i \in 0..NumServer] ==
            IF i = 0 THEN <<<<>>, Servers>>
            ELSE LET n == CHOOSE n \in F[i-1][2]: TRUE
                 IN <<Append(F[i-1][1], matchIndex'[me][n]), F[i-1][2] \ {n}>>
        sorted_match_idx == SortSeq(F[NumServer][1], LAMBDA x, y: x > y)
        commit == sorted_match_idx[NumServer \div 2 + 1]
        can_send == tempInflight[me][m.src] = 0
        old_pause == \/ inflight[me][m.src] /= 0
                     \/ /\ progress[me][m.src][1] = Probe
                        /\ progress[me][m.src][2] = TRUE
        empty_entries == LogCount(succ_rsp.data.entries) = 0
    IN IF /\ commit > commitIndex[me]
          /\ currentTerm[me] = LogGetTerm(log[me], commit, "AdvanceCommitIdx")
       THEN /\ SetCommitIdx(me, commit) \* commit change, maybe send_bcast
            /\ FlushSendAppendentries(me, m, tempNextIndex, tempInflight, <<"RecvAppendentriesResponse", "commit change", m.dst, m.src, m>>)
       ELSE /\ UNCHANGED commitIndex    
            /\ IF can_send
               THEN IF old_pause
                    THEN /\ NetUpdate2(NetReplyMsg(succ_rsp, m), <<"RecvAppendentriesResponse", "commit still send", m.dst, m.src, m>>) 
                         /\ IF ~empty_entries
                            THEN UpdateInflight(me, m.src, succ_rsp.data.entries[1].index) 
                            ELSE UpdateInflight(me, m.src, 0) 
                         /\ IF empty_entries  
                            THEN nextIndex' = tempNextIndex
                            ELSE UpdateNextIdx(me, m.src, succ_rsp.data.entries[1].index + 1)
                    ELSE IF empty_entries
                         THEN /\ NetUpdate2(NetDelMsg(m), <<"RecvAppendentriesResponse", "commit still pause", m.dst, m.src, m>>) 
                              /\ UpdateInflight(me, m.src, 0) 
                              /\ nextIndex' = tempNextIndex
                         ELSE /\ NetUpdate2(NetReplyMsg(succ_rsp, m), <<"RecvAppendentriesResponse", "commit still send", m.dst, m.src, m>>) 
                              /\ UpdateInflight(me, m.src, succ_rsp.data.entries[1].index) 
                              /\ UpdateNextIdx(me, m.src, succ_rsp.data.entries[1].index + 1)
               ELSE /\ NetUpdate2(NetDelMsg(m), <<"RecvAppendentriesResponse", "commit still pause", m.dst, m.src, m>>) 
                    /\ UNCHANGED inflight
                    /\ nextIndex' = tempNextIndex

\* maybe_decr_to @ progress.rs
maybe_decr_to(dst, src, m, next_probe_index) == 
    LET rejected == m.data.index
        match_hint == m.data.reject_hint
    IN  IF progress[dst][src][1] = Replicate
        THEN IF rejected <= matchIndex[dst][src]
             THEN /\ UNCHANGED << nextIndex, progress>>
                  /\ NetUpdate2(NetDelMsg(m), <<"RecvAppendentriesResponse", "replicate:  stale", dst, src, m>>)
             ELSE /\ UpdateNextIdx(dst, src, matchIndex[dst][src] + 1)
                  /\ LET one_rsp == AppendEntriesNext(dst, src, nextIndex')
                     IN /\ NetUpdate2(NetReplyMsg(one_rsp, m), <<"RecvAppendentriesResponse", "replicate trun to probe", dst, src, m>>)
                        /\ IF Len(one_rsp.data.entries) = 0
                           THEN UpdateProgress(dst, src, <<Probe, FALSE>>)
                           ELSE UpdateProgress(dst, src, <<Probe, TRUE>>)
        ELSE /\ IF \/ nextIndex[dst][src] = 0
                   \/ nextIndex[dst][src] - 1 /= rejected
                THEN /\ UNCHANGED << nextIndex, progress>>
                     /\ NetUpdate2(NetDelMsg(m), <<"RecvAppendentriesResponse", "probe: stale", dst, src, m>>)
                ELSE LET new_match == Min(rejected, next_probe_index + 1)
                         new_next_idx == Max(new_match, 1)  
                         one_rsp == AppendEntriesNext(dst, src, nextIndex')
                    IN /\ UpdateNextIdx(dst, src, new_next_idx)
                       /\ NetUpdate2(NetReplyMsg(one_rsp, m), <<"RecvAppendentriesResponse", "probe: update next", dst, src, m>>)
                       /\ IF Len(one_rsp.data.entries) = 0
                          THEN UpdateProgress(dst, src, <<Probe, FALSE>>)
                          ELSE UpdateProgress(dst, src, <<Probe, TRUE>>)

\* func: handle_append
HandleMsgAER(m) ==  
    LET resp == m.data
        src == m.src
        dst == m.dst
        stale == resp.term < currentTerm[dst]
        demote == resp.term > currentTerm[dst]
        need_optimize == resp.reject /\ resp.log_term > 0
        next_probe_index == find_conflict_by_term(dst, resp.reject_hint, resp.log_term)[1]   
        failReason ==
            IF stale THEN "stale message ignore" ELSE
            IF resp.term > currentTerm[dst] THEN "term is smaller" ELSE
            IF raftState[dst] /= Leader THEN "not leader" ELSE
            IF need_optimize THEN "retry" ELSE "success"
    IN      IF failReason /= "success"
            THEN IF failReason = "stale message ignore" \* drop stale message
                 THEN /\ UNCHANGED <<noNetVars>>
                      /\ NetUpdate2(NetDelMsg(m), <<"RecvAppendentriesResponse", "stale message ignore", dst, src, m>>)
                 ELSE IF failReason = "term is smaller" \* Received a newer letter and became a follower
                      THEN /\ BecomeFollower(dst, resp.term) 
                           /\ NetUpdate2(NetDelMsg(m), <<"RecvAppendentriesResponse", "term is smaller", dst, src, m>>)
                           /\ UNCHANGED <<check_quorum, logVars>>
                      ELSE IF failReason = "not leader" \* node not leader, drop the message
                           THEN /\ UNCHANGED <<noNetVars>>
                                /\ NetUpdate2(NetDelMsg(m), <<"RecvAppendentriesResponse", "not leader", dst, src, m>>)
                           ELSE IF failReason = "retry" \* m.reject
                                THEN /\ UNCHANGED <<serverVars, candidateVars, leader_id, check_quorum, logVars, matchIndex, inflight>>
                                     /\ maybe_decr_to(dst, src, m, next_probe_index)   
                                ELSE Assert(FALSE, <<"handle aer Unseen error situation", failReason>>)
            ELSE \* success
                /\ UNCHANGED <<leader_id, log, serverVars, candidateVars, check_quorum>>
                /\ LET  prboeToReplicate == progress[dst][src][1] = Probe
                        nextToUpdate == Max(resp.index + 1, nextIndex[dst][src]) \* The simulation here is that a call to maybe_update in handle_append_response may update next_idx, but since it will be changed again in prepare_entries, a temporary variable is needed to retrieve the corresponding entries.
                        tempNextIndex == [nextIndex EXCEPT ![dst][src] = nextToUpdate]
                        \* The temp nextIndex is also needed here.
                        one_rsp == AppendEntriesNext(dst, src, tempNextIndex)
                        repCanSend == inflight[dst][src] <= resp.index  \* The number of the arriving packet is stored in inflight, and in raft.rs, the replicate state will be free_to, so we'll simulate it directly here.
                        tempInflight == IF prboeToReplicate
                                        THEN [inflight EXCEPT ![dst][src] = 0]
                                        ELSE IF repCanSend 
                                             THEN [inflight EXCEPT ![dst][src] = 0]
                                             ELSE inflight  
                   IN /\ IF  prboeToReplicate
                         THEN UpdateProgress(dst, src, <<Replicate, FALSE>>)
                         ELSE UNCHANGED progress
                      /\ IF resp.index > matchIndex[dst][src]
                         THEN /\ UpdateMatchIdx(dst, src, resp.index)
                              /\ AdvanceCommitIdx(dst, m, one_rsp, tempNextIndex, tempInflight) \* Here we need to update the progress and nextIndex status according to the content of the letter, corresponding to the handle_append_response of the maybe_update to maybe_commit processing logic
                         ELSE /\ UNCHANGED << matchIndex, commitIndex, inflight, nextIndex>>
                              /\ NetUpdate2(NetDelMsg(m), <<"RecvAppendentriesResponse", "maybe_update_fail", dst, src, m>>)  

(***************************************************************************)
(* Recv heartBeat                                                          *)
(***************************************************************************)

\* func: handle_heartbeat
HandleMsgHB(m) ==  
    LET data == m.data
        src == m.src
        dst == m.dst
        demote == currentTerm[dst] < data.term
        stale == data.term < currentTerm[dst]
        rsp == HeartBeatResponse(m)
    IN IF stale
       THEN /\ UNCHANGED noNetVars
            /\ NetUpdate2(NetDelMsg(m), <<"RecvHeartBeat", "stale message ignore", dst, src, m>>)
       ELSE /\ IF \/ demote
                  \/ raftState[dst] = Candidate
               THEN /\ BecomeFollowerWithLeader(dst, data.term, src) 
                    /\ UNCHANGED <<check_quorum>>
               ELSE UNCHANGED <<serverVars, nodeVars, candidateVars, leaderVars>>
            /\ UNCHANGED <<log>>
            /\ SetCommitIdx(dst, data.commit)
            /\ NetUpdate2(NetReplyMsg(rsp, m), <<"RecvHeartBeat", "success", dst, src, m>>)
           
(***************************************************************************)
(* Recv HeartBeatResponse                                                  *)
(***************************************************************************)
\* func: handle_heartbeat_response
HandleMsgHBR(m) ==  
    LET resp == m.data
        src == m.src
        dst == m.dst
        demote == resp.term > currentTerm[dst]
        stale == resp.term < currentTerm[dst]
    IN IF stale
       THEN /\ UNCHANGED noNetVars
            /\ NetUpdate2(NetDelMsg(m), <<"RecvHeartBeatResponse", "stale message ignore", dst, src, m>>)
       ELSE IF demote
            THEN /\ UNCHANGED << logVars, check_quorum>>
                    /\ BecomeFollower(dst, resp.term)
                    /\ NetUpdate2(NetDelMsg(m), <<"RecvHeartBeatResponse", "term is smaller", dst, src, m>>) 
            ELSE /\ UNCHANGED <<serverVars, candidateVars, leader_id, check_quorum,  matchIndex, logVars, progress>> 
                 /\  IF matchIndex[dst][src] < LogCount(log[dst]) 
                     THEN LET req_msg == AppendEntriesNext(dst, src, nextIndex)
                              send_entry == req_msg.data.entries
                              isReplicate == progress[dst][src][1] = Replicate
                              inflightToUpdate == IF send_entry /= <<>> 
                                                  THEN send_entry[1].index
                                                  ELSE 0 
                              nextIndexToUpdate == IF isReplicate
                                                   THEN IF send_entry /= <<>>
                                                        THEN nextIndex[dst][src] + 1
                                                        ELSE nextIndex[dst][src]
                                                   ELSE nextIndex[dst][src]
                          IN  /\ NetUpdate2(NetReplyMsg(req_msg, m), <<"RecvHeartBeatResponse",  "send append", dst, src, m>>) 
                              /\ UpdateInflight(dst, src ,inflightToUpdate)
                              /\ UpdateNextIdx(dst, src, nextIndexToUpdate)
                     ELSE /\ NetUpdate2(NetDelMsg(m), <<"RecvHeartBeatResponse", "not send", dst, src, m>>) 
                          /\ UpdateInflight(dst, src ,0)
                          /\ UNCHANGED nextIndex


\* in step_leader: msg_propose
RecvEntry(n, data) ==  
    /\ raftState[n] = Leader
    /\ UNCHANGED <<serverVars, candidateVars, leader_id, check_quorum, progress, commitIndex>>
    /\ LET ety == [ term |-> currentTerm[n], data |-> data, index |->  LogCount(log[n]) + 1]
       IN log' = Update(log, n, LogAppend(log[n], ety))
    /\ IF matchIndex[n][n] < LogCount(log'[n])  
       THEN UpdateMatchIdx(n, n, LogCount(log'[n]))
       ELSE UNCHANGED matchIndex    
    /\ FlushSendAppendentries(n, Nil, nextIndex, inflight, <<"RecvEntry", n, data>>)


(***************************************************************************
  restart node
 ***************************************************************************)

\* Server i restarts. Only currentTerm/votedFor/log restored (i.e. unchanged).
\* NOTE: snapshot variables are considered as parts of log
\* NOTE: last applied index should be cleared here if modelled.
Restart(i) ==
    /\ UNCHANGED <<currentTerm, votedFor, logVars, check_quorum>>
    /\ raftState'       = [raftState           EXCEPT ![i] = Follower ]
    /\ leader_id'       = [ leader_id    EXCEPT ![i] = Nil]
    /\ voted_for_me'    = [ voted_for_me EXCEPT ![i] = {} ]
    /\ voted_reject'    = [ voted_reject EXCEPT ![i] = {} ]
    /\ nextIndex'         = [ nextIndex         EXCEPT ![i] = [j \in Servers |-> 1 ]]
    /\ matchIndex'        = [ matchIndex        EXCEPT ![i] = [j \in Servers |-> 0 ]]
    /\ progress'        = [ progress         EXCEPT ![i] = [j \in Servers |-> <<Probe, FALSE>>]]
    /\ inflight'        = [ inflight        EXCEPT ![i] = [j \in Servers |-> 0 ]]

(***************************************************************************
  State constraints
 ***************************************************************************)
 \* Here are some state limits to prevent state explosion due to control tla+
GetRealLogLen(curLog) == SelectSeq(curLog, LAMBDA i: i.data /= NoOp)
GetMaxLogLen == Len(log[CHOOSE i \in Servers: \A j \in Servers \ {i}:
                            GetRealLogLen(log[i]) >= GetRealLogLen(log[j])])
GetMaxTerm == currentTerm[CHOOSE i \in Servers: \A j \in Servers \ {i}:
                            currentTerm[i] >= currentTerm[j]]

ScSent == CheckParameterMax(netman.n_sent, "MaxSentMsgs")
ScRecv == CheckParameterMax(netman.n_recv, "MaxRecvMsgs")
ScWire == CheckParameterMax(netman.n_wire, "MaxWireMsgs")
\* ScLog  == CheckParameterMax(GetMaxLogLen,  "MaxLogLength")
\* ScTerm == CheckParameterMax(GetMaxTerm,    "MaxTerm")
ScPart == CheckParameterMax(netman.n_part, "MaxPartitionTimes")
ScCure == CheckParameterMax(netman.n_cure, "MaxCureTimes")
ScOp   == CheckParameterMax(netman.n_op,   "MaxClientOperationsTimes")
ScAe   == CheckParameterMax(netman.n_ae,   "MaxAppendEntriesTimes")
ScElec == CheckParameterMax(netman.n_elec, "MaxElectionTimes")
ScDrop == CheckParameterMax(netman.n_drop, "MaxDropTimes")
ScDup  == CheckParameterMax(netman.n_dup,  "MaxDupTimes")
ScRestart == CheckParameterMax(netman.n_restart,  "MaxRestart")
ScUnorder == CheckParameterMax(netman.n_unorder, "MaxUnorderTimes")

SC == /\ ScSent /\ ScRecv /\ ScWire /\ ScRestart
      /\ ScPart /\ ScCure /\ ScOp /\ ScAe /\ ScElec


(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)
ElectionSafety ==
    LET TwoLeader ==
            \E i, j \in Servers:
                /\ i /= j
                /\ currentTerm'[i] = currentTerm'[j]
                /\ raftState'[i] = Leader
                /\ raftState'[j] = Leader
    IN ~TwoLeader

LeaderAppendOnly ==
    \A i \in Servers:
        IF raftState[i] = Leader /\ raftState'[i] = Leader
        THEN LET curLog == log[i]
                 nextLog == log'[i]
             IN IF Len(nextLog) >= Len(curLog)
                THEN SubSeq(nextLog, 1, Len(curLog)) = curLog
                ELSE FALSE
        ELSE TRUE

LogMatching ==
  ~UNCHANGED log =>  \* check the safety only if log has unchanged to avoid unnecessary evaluation cost
    \A i, j \in Servers:
        IF i /= j
        THEN LET iLog == log'[i]
                 jLog == log'[j]
                 len == Min(Len(iLog), Len(jLog))
                 F[k \in 0..len] ==
                    IF k = 0 THEN <<>>
                    ELSE LET key1 == <<iLog[k].term, k>>
                             value1 == iLog[k].data
                             key2 == <<jLog[k].term, k>>
                             value2 == jLog[k].data
                             F1 == IF key1 \in DOMAIN F[k-1]
                                   THEN IF F[k-1][key1] = value1
                                        THEN F[k-1]
                                        ELSE F[k-1] @@ ( <<-1, -1>> :> <<key1, value1, F[k-1][key1]>> )
                                   ELSE F[k-1] @@ (key1 :> value1)
                             F2 == IF key2 \in DOMAIN F1
                                   THEN IF F1[key2] = value2
                                        THEN F1
                                        ELSE F1 @@ ( <<-1, -1>> :> <<key2, value2, F1[key2]>> )
                                   ELSE F1 @@ (key2 :> value2)
                         IN F2
             IN IF << -1, -1>> \notin DOMAIN F[len] THEN TRUE
                ELSE Assert(FALSE, <<i, j, F>>)
        ELSE TRUE

MonotonicCurrentTerm == \A i \in Servers: currentTerm' [i] >= currentTerm[i]

MonotonicCommitIdx == \A i \in Servers: commitIndex'[i] >= commitIndex[i]

MonotonicMatchIdx ==
    \A i \in Servers:
        IF raftState[i] = Leader /\ raftState'[i] = Leader  \* change
        THEN \A j \in Servers:  matchIndex'[i][j] >= matchIndex[i][j]
        ELSE TRUE

CommittedLogDurable ==
    \A i \in Servers:
        LET len     == Min(commitIndex'[i], commitIndex[i])
            logNext == SubSeq(log'[i], 1, len)
            logCur  == SubSeq(log[i], 1, len)
        IN IF len = 1 THEN TRUE
           ELSE /\ Len(logNext) >= len
                /\ Len(logCur) >= len
                /\ logNext = logCur

CommittedLogReplicatedMajority ==
     \A i \in Servers:
        IF raftState'[i] /= Leader \/ commitIndex'[i] <= 1
        THEN TRUE
        ELSE LET entries == SubSeq(log'[i], 1, commitIndex'[i])
                 len     == Len(entries)
                 nServer == Cardinality(Servers)
                 F[j \in 0..nServer] ==
                    IF j = 0
                    THEN <<{}, {}>>
                    ELSE LET k == CHOOSE k \in Servers: k \notin F[j-1][1]
                             logLenOk == LogCount(log'[k]) >= commitIndex'[i]
                             kEntries == SubSeq(log'[k], 1, commitIndex'[i])
                         IN IF /\ logLenOk
                               /\ entries = kEntries
                             THEN <<F[j-1][1] \union {k}, F[j-1][2] \union {k}>>
                             ELSE <<F[j-1][1] \union {k}, F[j-1][2]>>
             IN IsQuorum(F[nServer][2])

NextIdxGtMatchIdx ==
    \A i \in Servers:
        IF raftState'[i] = Leader
        THEN \A j \in Servers \ {i}: nextIndex'[i][j] > matchIndex'[i][j]
        ELSE TRUE

NextIdxGtZero ==
    \A i \in Servers:
        IF raftState'[i] = Leader
        THEN \A j \in Servers: nextIndex'[i][j] > 0
        ELSE TRUE

SelectSeqWithIdx(s, Test(_,_)) == 
    LET F[i \in 0..Len(s)] == 
        IF i = 0
        THEN <<>>
        ELSE IF Test(s[i], i)
             THEN Append(F[i-1], s[i])
             ELSE F[i-1]
    IN F[Len(s)]

FollowerLogLELeaderLogAfterAE ==
    LET cmd  == netcmd'[1]
        cmd1 == cmd[1]
        cmd2 == cmd[2]
        follower == cmd[3]
        leader   == cmd[4]
    IN IF cmd1 = "RecvAppendentries" /\ cmd2 \in { "success", "term Mismatch" }
       THEN IF log[follower] /= log'[follower]
            THEN LogCount(log'[follower]) <= LogCount(log'[leader])
            ELSE TRUE
       ELSE TRUE

CommitIdxLELogLen ==
    \A i \in Servers: commitIndex'[i] <= LogCount(log'[i])

LeaderCommitCurrentTermLogs ==
    \A i \in Servers:
        IF raftState'[i] = Leader
        THEN IF commitIndex[i] /= commitIndex'[i]
             THEN log'[i][commitIndex'[i]].term = currentTerm'[i]
             ELSE TRUE
        ELSE TRUE

NewLeaderTermNotInLog ==
    \A i \in Servers:
        IF raftState'[i] = Leader /\ raftState[i] /= Leader
        THEN \A j \in Servers \ {i}:
                \A n \in DOMAIN log'[j]:
                    log'[j][n].term /= currentTerm'[i]
        ELSE TRUE

LeaderTermLogHasGreatestIdx ==
    \A i \in Servers:
        IF raftState'[i] = Leader
        THEN \A j \in Servers \ {i}:
                LET IncTermLogCount(a, b) == IF a.term = currentTerm'[i] THEN b + 1 ELSE b
                IN FoldSeq(IncTermLogCount, 0, log'[i]) >= FoldSeq(IncTermLogCount, 0, log'[j])
        ELSE TRUE

CheckLeader ==
    \A i \in Servers:
       raftState[i] /= Leader

InvSequence == <<
    ElectionSafety,
    LeaderAppendOnly,
    LogMatching,
    MonotonicCurrentTerm,
    MonotonicCommitIdx,
    MonotonicMatchIdx,
    CommittedLogDurable,
    CommittedLogReplicatedMajority,
    NextIdxGtMatchIdx,
    NextIdxGtZero,
    \* CheckLeader
    FollowerLogLELeaderLogAfterAE,
    CommitIdxLELogLen,
    LeaderCommitCurrentTermLogs,
    NewLeaderTermNotInLog,
    LeaderTermLogHasGreatestIdx

>>

INV == Len(SelectSeqWithIdx(inv, LAMBDA x, y: ~x /\ y \notin netman.no_inv)) = 0



 (***************************************************************************
  Next actions
 ***************************************************************************)

DoElectionTimeout ==
    /\ PrePrune(netman.n_elec, "MaxElectionTimes")
    /\ \E n \in Servers: CheckStateIs(n, Follower) /\ BecomeCandidate(n)
    /\ inv' = InvSequence


DoHeartBeat ==
    /\ PrePrune(netman.n_hb, "MaxHeartBeatTimes")
    /\ \E n \in Servers:
        /\ raftState[n] = Leader
        /\ SendHeartBeatAll(n)
    /\ inv' = InvSequence


_DoRecvM(type, func(_)) ==
    /\ \E src, dst \in Servers:
        /\ src /= dst
        /\ LET m == NetGetMsg(src, dst)
           IN /\ m /= Nil
              /\ m.type = type
              /\ func(m)
    /\ inv' = InvSequence


DoHandleMsgRV == /\ _DoRecvM(M_RV, HandleMsgRV)

DoHandleMsgRVR == /\ _DoRecvM(M_RVR, HandleMsgRVR)
                  
DoHandleMsgAE == /\ _DoRecvM(M_AE, HandleMsgAE)

DoHandleMsgAER == /\ _DoRecvM(M_AER, HandleMsgAER)
                  
DoHandleMsgHB == /\ _DoRecvM(M_HB, HandleMsgHB)

DoHandleMsgHBR == /\ _DoRecvM(M_HBR, HandleMsgHBR)
                  
DoRecvEntry ==
    /\ PrePrune(netman.n_op, "MaxClientOperationsTimes")
    /\ \E n \in Servers, v \in Commands: RecvEntry(n, v)
    /\ inv' = InvSequence

\* DoNetworkDrop ==
\*     /\ PrePrune(NetGetDrop, "MaxDropTimes")
\*     /\ \E m \in msgs: 
\*         /\ NetUpdate2(NetDropMsg(m), <<"DoNetworkDrop", m.dst, m.src, m.seq>>)
\*         /\ UNCHANGED noNetVars
\*     /\ inv' = InvSequence

\* DoNetworkDup ==
\*     /\ PrePrune(NetGetDup, "MaxDupTimes")
\*     /\ \E m \in msgs: 
\*         /\ NetUpdate2(NetDupMsg(m), <<"DoNetworkDup", m.dst, m.src, m.seq>>)
\*         /\ UNCHANGED noNetVars
\*     /\ inv' = InvSequence

DoNetworkPartition ==
    /\ PrePrune(netman.n_part, "MaxPartitionTimes")
    /\ \E n \in Servers:
        /\ NetUpdate2(NetPartConn({n}), <<"DoNetworkPartition", n>>)
        /\ UNCHANGED noNetVars
    /\ inv' = InvSequence

DoNetworkCure ==
    /\ PrePrune(netman.n_cure, "MaxCureTimes")
    /\ NetIsParted
    /\ NetUpdate2(NetCureConn, <<"DoNetworkCure">>)
    /\ UNCHANGED noNetVars
    /\ inv' = InvSequence

DoRestart ==
    /\ PrePrune(netman.n_restart, "MaxRestart")
    /\ \E n \in Servers: 
        /\ Restart(n)
        /\ NetUpdate2(NetmanIncField("n_restart", NetNoAction1), <<"Dorestart", n>>)
    /\ inv' = InvSequence

Next == 
    \/ DoRestart
    \/ DoElectionTimeout
    \/ DoHeartBeat
    \/ DoHandleMsgRV
    \/ DoHandleMsgRVR
    \/ DoHandleMsgHB
    \/ DoHandleMsgHBR
    \/ DoHandleMsgAE
    \/ DoHandleMsgAER
    \/ DoRecvEntry
    \* \/ DoNetworkDrop
    \* \/ DoNetworkDup
    \/ DoNetworkPartition
    \/ DoNetworkCure



Spec == Init /\ [][Next]_vars
====
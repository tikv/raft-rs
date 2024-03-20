----------------- MODULE UdpNetwork ----------------------
EXTENDS TLC, Naturals, FiniteSets, Sequences
(***************************************************************************
  VARIABLES definitions: see InitUdpNetwork
 ***************************************************************************)

VARIABLES _msgs,    \* Messages in the network
          _netman,  \* Network manager
          _netcmd   \* Current network cmd


(* NULL_MSG: represent a null msg in some condition checkings 
    shoule be a model value if its type is constant *)
CONSTANT NULL_MSG
\*NULL_MSG == [ NULL_MSG |-> "" ]

---- \* Common functions

(*****)
(*  API InitUdpNetwork(nodes):
    - _msgs: init to empty set of msgs
        * format [seq |-> 0, src |-> s0, dst |-> s1, type |-> sth, data -> sth]
        * src and dst will be dropped when storing in _msgs
        * type and data are user defined fields
    _ _netman:
        - n_sent: number of msgs sent to network, to indicate next msg seq
        - n_recv: number of msgs delivered to server 
        - n_wire: number of msgs in network but not delivered yet
        - n_unorder: unordered failure times
        - n_drop: drop failure times
        - n_dup: duplicate failure times
    - _netcmd: <<"Init">> *)
(*****)


InitUdpNetworkNetman(nodes, cmd, additonalNetman) ==
    /\ _msgs = {}
    /\ _netman = additonalNetman @@ 
                  [ n_sent |-> 0, n_recv |-> 0, n_wire |-> 0,
                    n_unorder |-> 0, n_drop |-> 0, n_dup |-> 0 ]
    /\ _netcmd = <<cmd>>

InitUdpNetwork(nodes) == InitUdpNetworkNetman(nodes, "init", <<>>)

------ \* Update _netman functions

(***************************************************************************
  _NetGetHelper and _NetIncHelper: get, inc and dec member of _netman records
 ***************************************************************************)
_NetGetHelper(member) == _netman[member]
_NetIncHelper(member) == (member :> _netman[member] + 1)
_NetDecHelper(member) == (member :> _netman[member] - 1)
NetIncBy(member, number) == (member :> _netman[member] + number)

NetGetSent == _NetGetHelper("n_sent")
NetIncSent == _NetIncHelper("n_sent")
NetGetRecv == _NetGetHelper("n_recv")
NetIncRecv == _NetIncHelper("n_recv")
NetGetWire == _NetGetHelper("n_wire")
NetIncWire == _NetIncHelper("n_wire")
NetDecWire == _NetDecHelper("n_wire")
NetGetUnorder == _NetGetHelper("n_unorder")
NetIncUnorder == _NetIncHelper("n_unorder")
NetGetDrop == _NetGetHelper("n_drop")
NetIncDrop == _NetIncHelper("n_drop")
NetGetDup == _NetGetHelper("n_dup")
NetIncDup == _NetIncHelper("n_dup")

---- \* Network send and recv functions

(****)
(* API updater : 
    * return <<netman, msg, cmd>>  *)
(****)
NetmanIncField(field, updater) ==
    <<_NetIncHelper(field) @@ updater[1]>> @@ updater 


\*NetmanIncFieldWithoutUpdate(field) ==
\*    <<_NetIncHelper(field) , _msgs ,_netcmd>>


\* return <<added count, updated msgs>>
_AddMsgSeq(m, seq, msgs) == LET m_ == IF "seq" \in DOMAIN m  \* TODO: add partition, see wraft
                                THEN {[ m EXCEPT !["seq"] = seq ]}  \* inc dup to indicate it is a duplicate msg
                                ELSE {m @@ [ seq |-> seq]}
                            IN <<1, msgs \union m_>>

\* Add msg to msgs, increase scr.nMessage.
_AddMsg(m, msgs) == LET seq == NetGetSent + 1
                   IN _AddMsgSeq(m, seq, msgs)



(****)
(* _BatchAddMsgs: batch add multi messages to msgs
    * return <<added count, updated msgs>>
    * set global seq to each msg m 
*)
(****)
_BatchAddMsgs(ms, msgs)==
    LET F[i \in 0 .. Len(ms)] ==
        IF i = 0 THEN <<0, msgs, <<"msg_batch_add">> >>
        ELSE LET m == ms[i]
                 seq == NetGetSent + F[i-1][1] + 1
                 res == _AddMsgSeq(m, seq ,F[i-1][2])
             IN << res[1] + F[i-1][1], res[2], Append(F[i-1][3],
                    IF res[1] = 1 THEN <<"ok", m.src, m.dst, seq>>
                    ELSE <<"dropped", m.src, m.dst, seq>>) >>
    IN F[Len(ms)]


(***************************************************************************
  _DelMsg: delete m from msgs return <<deleted count, updated msgs>>
 ***************************************************************************)
\* Del msg from msgs.
_DelMsg(m, msgs) ==
    IF m \in msgs
    THEN <<1, msgs\ {m}>>
    ELSE Assert(FALSE, "Delmsg: not in network")

(***************************************************************************
  _ReplyMsg: delete request from msgs and then add reponse to msgs
    * return <<decreased wire msgs count, msgs>>
 ***************************************************************************)
\* Combination of Send and Discard.
_ReplyMsg(response, request, msgs) ==
    LET del == _DelMsg(request, msgs)
        add == _AddMsg(response, del[2])
    IN <<del[1] - add[1], add[2]>>

(***************************************************************************
  API NetGetMsg: Get msg from src -> dst FIFO head
    * return msg m
    真的需要实现吗？？ 讨论
 ***************************************************************************)
\* NetGetMsg(src, dst) == _GetMsg(src, dst, _msgs)

(* inc unorder *)

IsFirstMsg(m) ==
    LET myMsg == { i \in _msgs: i.dst = m.dst }  \* 是否需要考虑src？
        first == CHOOSE i \in myMsg: i.seq <= m.seq
    IN  first = m

NetIncRecvCheckUnorder(m) ==
    IF IsFirstMsg(m)
    THEN NetIncRecv
    ELSE NetIncRecv @@ NetIncUnorder


(***************************************************************************
  API NetDelMsg: Del msgs of m
    * return <<netman, msgs, cmd>>
    * update with NetUpdate
 ***************************************************************************)
NetDelMsg(m) == 
    LET res == _DelMsg(m, _msgs)
    IN <<NetDecWire @@ NetIncRecvCheckUnorder(m), res[2], <<"msg_del", m.dst, m.src, m.seq>> >>

(***************************************************************************
  API NetDropMsg: Drop msgs of m
    * return <<netman, msgs, cmd>>
    * update with NetUpdate
 ***************************************************************************)
NetDropMsg(m) == 
    LET res == _DelMsg(m, _msgs)
    IN <<NetDecWire @@ NetIncDrop , res[2], <<"msg_drop", m.dst, m.src, m.seq>> >>


(***************************************************************************
  API NetDupMsg: Duplicate msgs of m
    * return <<netman, msgs, cmd>>
    * update with NetUpdate
 ***************************************************************************)
NetDupMsg(m) == 
    LET res == _AddMsg(m, _msgs)
    IN <<NetIncSent @@ NetIncWire @@ NetIncDup, res[2], <<"msg_dup", m.dst, m.src, m.seq>> >>


(****)
(* API NetAddMsg : add m into msgs
    * return <<netman, msg, cmd>>  *)
(****)
NetAddMsg(m) == 
    LET res == _AddMsg(m, _msgs)
    IN IF res[1] = 1
       THEN <<NetIncSent @@ NetIncWire, res[2], <<"msg_add",  m.src, m.dst>> >>  \* here we do not need seq, because we put in network then sort
       ELSE <<_netman, res[2], <<"msg_add_dropped",  m.src, m.dst>> >>

NetReplyMsg(response, request) ==
    LET res == _ReplyMsg(response, request, _msgs)
    IN  IF res[1] = 0
        THEN <<NetIncSent @@ NetIncRecvCheckUnorder(request), res[2], 
                << "msg_reply", request.dst, request.src, request.seq>> >>
        ELSE <<NetDecWire @@ NetIncRecvCheckUnorder(request), res[2],
                <<"msg_reply_dropped", request.dst, request.src, request.seq>> >>


(***************************************************************************
  API NetBatchAddMsg: batch add messages ms to msgs
 ***************************************************************************)
NetBatchAddMsg(ms) == 
    LET res == _BatchAddMsgs(ms, _msgs)
    IN <<NetIncBy("n_sent", res[1]) @@ NetIncBy("n_wire", res[1]), res[2], res[3]>>

(***************************************************************************
  API NetReplyBatchAddMsg: remove request and batch add ms to msgs
 ***************************************************************************)
NetReplyBatchAddMsg(ms, request) ==
    LET del == _DelMsg(request, _msgs)
        add == _BatchAddMsgs(ms, del[2])
    IN <<NetIncBy("n_sent", add[1]) @@ NetIncBy("n_wire", add[1]-del[1]) @@ NetIncRecvCheckUnorder(request),
        add[2], Append(<<"msg_batch_add_reply", request.dst, request.src, request.seq>>, add[3])>>

(***************************************************************************
  API NetNoAction: Network state unchanged
    * return <<netman, msg>>
 ***************************************************************************)
NetNoAction(cmd) == <<_netman, _msgs, cmd>>


NetUpdate(args) ==
    /\ _netman' = args[1] @@ _netman
    /\ _msgs' = args[2]
    /\ IF Len(args) = 3
       THEN _netcmd' = args[3]
       ELSE _netcmd' = <<"noop">>

NetUpdate2(args, cmd) ==
    /\ _netman' = args[1] @@ _netman
    /\ _msgs' = args[2]
    /\ IF Len(args) = 3
       THEN _netcmd' = <<cmd, args[3]>>
       ELSE _netcmd' = <<cmd>>


====
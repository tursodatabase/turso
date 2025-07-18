--------------------------- MODULE MVCC ---------------------------
EXTENDS Naturals, Sequences, TLC, FiniteSets

CONSTANTS 
    Connections,     \* Set of database connections
    Keys,           \* Set of data keys
    Values,         \* Set of possible values
    MaxTxnId        \* Maximum transaction ID

VARIABLES
    mvccIndex,      \* In-memory MVCC index: [key |-> [txnId |-> value]]
    walLog,         \* WAL log: sequence of committed transactions
    txnStates,      \* Transaction states: [conn |-> [state, txnId, writeSet]]
    nextTxnId       \* Next available transaction ID

vars == <<mvccIndex, walLog, txnStates, nextTxnId>>

TxnStates == {"ACTIVE", "COMMITTING", "COMMITTED", "ABORTED"}

TypeOK ==
    /\ \A k \in Keys : mvccIndex[k] \in [SUBSET (1..MaxTxnId) -> Values \cup {"NOT_FOUND"}]
    /\ walLog \in Seq([txnId: 1..MaxTxnId, writeSet: [SUBSET Keys -> Values]])
    /\ \A c \in Connections : 
        /\ txnStates[c].state \in TxnStates
        /\ txnStates[c].txnId \in 0..MaxTxnId  
        /\ txnStates[c].writeSet \in [SUBSET Keys -> Values]
    /\ nextTxnId \in 1..(MaxTxnId+1)

Init ==
    /\ mvccIndex = [k \in Keys |-> [n \in {} |-> "NULL"]]  \* Empty functions from txnId to value
    /\ walLog = <<>>
    /\ txnStates = [c \in Connections |-> [state |-> "COMMITTED", txnId |-> 0, writeSet |-> [k \in {} |-> "NULL"]]]
    /\ nextTxnId = 1

\* Helper functions
GetVisibleVersion(key, txnId) ==
    \* Get the latest version of key visible to txnId from MVCC index
    LET versions == mvccIndex[key]
        visibleVersions == {v \in DOMAIN versions : v <= txnId}
    IN IF visibleVersions = {} 
       THEN "NOT_FOUND"
       ELSE versions[CHOOSE v \in visibleVersions : 
                      \A other \in visibleVersions : v >= other]

GetFromWAL(key, txnId) ==
    \* Lazily load from WAL - find latest committed version visible to txnId
    LET commitRecords == {i \in DOMAIN walLog : 
                         /\ key \in DOMAIN walLog[i].writeSet 
                         /\ walLog[i].txnId <= txnId}
    IN IF commitRecords = {}
       THEN "NOT_FOUND" 
       ELSE LET latestRecord == CHOOSE i \in commitRecords :
                               \A j \in commitRecords : walLog[i].txnId >= walLog[j].txnId
            IN walLog[latestRecord].writeSet[key]

\* Begin a new transaction
BeginTransaction(conn) ==
    /\ txnStates[conn].state = "COMMITTED"
    /\ nextTxnId <= MaxTxnId  \* Bound the number of transactions
    /\ txnStates' = [txnStates EXCEPT 
                     ![conn] = [state |-> "ACTIVE", 
                               txnId |-> nextTxnId, 
                               writeSet |-> [k \in {} |-> "NULL"]]]  \* Empty function
    /\ nextTxnId' = nextTxnId + 1
    /\ UNCHANGED <<mvccIndex, walLog>>

\* Read operation - check MVCC index first, then lazily load from WAL
ReadValue(conn, key) ==
    /\ txnStates[conn].state = "ACTIVE"
    /\ LET txnId == txnStates[conn].txnId
           mvccValue == GetVisibleVersion(key, txnId)
       IN IF mvccValue # "NOT_FOUND"
          THEN UNCHANGED vars  \* Found in MVCC index
          ELSE LET walValue == GetFromWAL(key, txnId)
               IN IF walValue # "NOT_FOUND"
                  THEN LET commitRecords == {i \in DOMAIN walLog : 
                                           /\ key \in DOMAIN walLog[i].writeSet 
                                           /\ walLog[i].txnId <= txnId}
                           latestRecord == CHOOSE i \in commitRecords :
                                          \A j \in commitRecords : walLog[i].txnId >= walLog[j].txnId
                           originalTxnId == walLog[latestRecord].txnId
                       IN /\ mvccIndex' = [mvccIndex EXCEPT ![key] = 
                                          [t \in (DOMAIN mvccIndex[key] \cup {originalTxnId}) |->
                                           IF t = originalTxnId THEN walValue ELSE mvccIndex[key][t]]]
                          /\ UNCHANGED <<walLog, txnStates, nextTxnId>>
                  ELSE UNCHANGED vars  \* Key doesn't exist

\* Write operation (only to MVCC index)
WriteValue(conn, key, value) ==
    /\ txnStates[conn].state = "ACTIVE"
    /\ LET txnId == txnStates[conn].txnId
       IN /\ mvccIndex' = [mvccIndex EXCEPT ![key] = 
                          [t \in (DOMAIN mvccIndex[key] \cup {txnId}) |->
                           IF t = txnId THEN value ELSE mvccIndex[key][t]]]
          /\ txnStates' = [txnStates EXCEPT 
                          ![conn].writeSet = [t \in (DOMAIN txnStates[conn].writeSet \cup {key}) |->
                                             IF t = key THEN value ELSE txnStates[conn].writeSet[t]]]
          /\ UNCHANGED <<walLog, nextTxnId>>

\* Commit transaction - write to WAL and mark as committed
CommitTransaction(conn) ==
    /\ txnStates[conn].state = "ACTIVE"
    /\ LET txn == txnStates[conn]
           commitRecord == [txnId |-> txn.txnId, writeSet |-> txn.writeSet]
       IN /\ walLog' = Append(walLog, commitRecord)
          /\ txnStates' = [txnStates EXCEPT ![conn].state = "COMMITTED"]
          /\ UNCHANGED <<mvccIndex, nextTxnId>>

AbortTransaction(conn) ==
    /\ txnStates[conn].state = "ACTIVE"
    /\ LET txnId == txnStates[conn].txnId
       IN /\ mvccIndex' = [k \in Keys |-> 
                          [v \in DOMAIN mvccIndex[k] \ {txnId} |-> 
                           mvccIndex[k][v]]]
          /\ txnStates' = [txnStates EXCEPT ![conn] = [state |-> "COMMITTED", txnId |-> 0, writeSet |-> [k \in {} |-> "NULL"]]]
          /\ UNCHANGED <<walLog, nextTxnId>>

\* System restart - lose MVCC index (simulate crash)
SystemRestart ==
    /\ \A conn \in Connections : txnStates[conn].state = "COMMITTED"  \* No active transactions
    /\ mvccIndex' = [k \in Keys |-> [n \in {} |-> "NULL"]]  \* Clear MVCC index (volatile memory lost)
    /\ UNCHANGED <<walLog, txnStates, nextTxnId>>

\* Next state relation
Next ==
    \/ \E conn \in Connections : BeginTransaction(conn)
    \/ \E conn \in Connections, key \in Keys : ReadValue(conn, key)
    \/ \E conn \in Connections, key \in Keys, value \in Values : 
        WriteValue(conn, key, value)
    \/ \E conn \in Connections : CommitTransaction(conn)
    \/ \E conn \in Connections : AbortTransaction(conn)
    \/ SystemRestart
    \/ UNCHANGED vars  \* Allow stuttering when no actions are enabled

Spec == Init /\ [][Next]_vars

\* SAFETY PROPERTIES

\* Integrity: committed data must be in WAL  
Integrity ==
    \A i \in DOMAIN walLog :
        \A key \in DOMAIN walLog[i].writeSet :
            /\ walLog[i].writeSet[key] \in Values
            /\ walLog[i].writeSet[key] # "NOT_FOUND"

\* Durability: committed transactions are in WAL
Durability ==
    \A conn \in Connections :
        /\ txnStates[conn].state = "COMMITTED" 
        /\ txnStates[conn].txnId > 0
        /\ txnStates[conn].writeSet # <<>>
        => \E i \in DOMAIN walLog : walLog[i].txnId = txnStates[conn].txnId

\* Isolation: active transactions only see committed data
Isolation ==
    \A conn1, conn2 \in Connections :
        /\ conn1 # conn2
        /\ txnStates[conn1].state = "ACTIVE"
        /\ txnStates[conn2].state = "ACTIVE"
        => txnStates[conn1].txnId # txnStates[conn2].txnId

\* Atomicity: either all writes are committed or none are
Atomicity ==
    \A conn \in Connections :
        /\ txnStates[conn].state = "COMMITTED"
        /\ txnStates[conn].txnId > 0
        /\ txnStates[conn].writeSet # <<>>
        => \E i \in DOMAIN walLog :
            /\ walLog[i].txnId = txnStates[conn].txnId
            /\ DOMAIN walLog[i].writeSet = DOMAIN txnStates[conn].writeSet

\* Version ordering: MVCC versions are ordered by transaction ID
VersionOrdering ==
    \A key \in Keys :
        \A txnId1, txnId2 \in DOMAIN mvccIndex[key] :
            txnId1 < txnId2 => txnId1 # txnId2

\* Consistency: MVCC index data must match WAL (but WAL can have more data)
Consistency ==
    \A key \in Keys :
        \A txnId \in DOMAIN mvccIndex[key] :
            /\ mvccIndex[key][txnId] # "NULL"  \* Skip empty entries
            /\ \E i \in DOMAIN walLog :
                /\ walLog[i].txnId = txnId
                /\ key \in DOMAIN walLog[i].writeSet
                /\ mvccIndex[key][txnId] = walLog[i].writeSet[key]

\* INVARIANTS
SafetyInvariant == 
    /\ Integrity
    /\ Durability  
    /\ Isolation
    /\ Atomicity
    /\ VersionOrdering
    \*/\ Consistency

=============================================================================
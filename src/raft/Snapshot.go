package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term              int    
	LeaderId          int   
	LastIncludedIndex int 
	LastIncludedTerm  int  
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.ToFollower(args.Term)
	}
	rf.lastHeartBeat = time.Now()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 6. if existing log entry has the same index and term as snapshot's last included entry,
	// retain log entries following it and reply
	if args.LastIncludedIndex < rf.logLen()-1 {
		// the args.LastIncludedIndex log has agreed, if there are more logs, just retain them
		rf.log = append(make([]LogEntry, 0), rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
	} else {
		// 7. discard the entire log
		// empty log use for AppendEntries RPC consistency check
		rf.log = []LogEntry{{args.LastIncludedTerm, nil}}
	}

	// update snapshot state and persist them
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.persist(args.Data)

	// force the follower's log catch up with leader
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	if rf.lastApplied > rf.lastIncludedIndex {
		return
	}

	// 8. reset state machine using snapshot contents (and load snapshot's cluster configuration)
	applyMsg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: rf.lastIncludedIndex}
	rf.applyChProxy <- applyMsg
}

//need lock
func (rf *Raft) sendInstallSnapshot(peer int) {

	//rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)


	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.serverState != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.ToFollower(reply.Term)
		return
	}
	rf.updateMatchIndex(peer, rf.lastIncludedIndex)
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("snapshot")

	rf.mu.Lock()
	DPrintf("snapshot in")
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.getLastLogIndex(){
		return
	}
	DPrintf("snapshot server%v: %v %v\n",rf.me, index, rf.getLastLogIndex() + 1)

	rf.log = rf.getLogSlice(index, rf.getLastLogIndex() + 1)

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLogEntry(index).Term
	rf.persist(snapshot)
}
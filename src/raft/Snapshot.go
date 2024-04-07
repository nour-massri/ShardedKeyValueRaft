package raft

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	// Offset         int    // byte offset where chunk is positioned in the snapshot file
	// Done           bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
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
	send(rf.electionResetCh)

	// check snapshot may expire by lock competition, otherwise rf.logs may overflow below
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

func (rf *Raft) sendInstallSnapshot(peer int)  {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		LeaderId:          rf.me,
		Data:              rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
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
	rf.updatePeerMatch(peer, rf.lastIncludedIndex)
	rf.updateCommitIndex()

}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	rf.log = append(make([]LogEntry, 0), rf.log[index-rf.lastIncludedIndex:]...)
	// update new lastIncludedIndex and lastIncludedTerm
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLogEntry(index).Term
	// save snapshot
	rf.persist(snapshot)
}

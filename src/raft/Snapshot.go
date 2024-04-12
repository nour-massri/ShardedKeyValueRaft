package raft

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm


	if args.Term > rf.currentTerm {
		rf.ToFollower(args.Term)
	}

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	send(rf.electionResetCh)

	//5. Save snapshot file, discard any existing or partial snapshot
	//with a smaller index 
	//in other words discard installsnapshot if smaller index
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	if args.LastIncludedIndex + 1 < rf.logLen() {
		// 6. if existing log entry has the same index and term as snapshot's last included entry,
		// retain log entries following it and reply
		rf.log = rf.getLogSlice(args.LastIncludedIndex, rf.logLen())
	} else {
		// 7. discard the entire log
		rf.log = []LogEntry{{args.LastIncludedTerm, nil}}
	}

	// 8. reset state machine using snapshot contents (and load snapshot's cluster configuration)
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.persist(args.Data)


	if rf.lastApplied <= rf.lastIncludedIndex {
		// 8. reset state machine using snapshot contents (and load snapshot's cluster configuration)
		rf.applyChProxy  <- ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: rf.lastIncludedIndex}
	}

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

	rf.log = rf.getLogSlice(index, rf.logLen())
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLogEntry(index).Term
	rf.persist(snapshot)
}

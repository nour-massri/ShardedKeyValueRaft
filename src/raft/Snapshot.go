/*
package raft

import (

	"time"

)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

	func (rf *Raft) Snapshot(index int, snapshot []byte) {
		DPrintf("snapshot")

		// Your code here (3D).
		rf.mu.Lock()
		DPrintf("snapshot in")
		defer rf.mu.Unlock()
		defer rf.persist()

		if index <= rf.lastIncludedIndex || index > rf.getLastLogIndex(){
			return
		}
		DPrintf("snapshot server%v: %v %v\n",rf.me, index, rf.getLastLogIndex() + 1)

		rf.lastIncludedIndex = index
		rf.lastIncludedTerm = rf.getLogEntry(index).Term
		rf.snapshot = snapshot

		rf.log = rf.getLogSlice(index, rf.getLastLogIndex()+1)

}

	func (rf *Raft) readSnapshot(snapshot []byte) {
		if snapshot == nil || len(snapshot) < 1 {
			return
		}
		DPrintf("reading valid snapshot")
		rf.snapshot = snapshot
		rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: snapshot}
	}

	type InstallSnapshotsArgs struct {
		Term int
		LeaderId int
		LastIncludedIndex int
		LastIncludedTerm int
		Data []byte
	}

	type InstallSnapshotsReply struct {
		Term int
	}

// example InstallSnapshot RPC handler.

	func (rf *Raft) InstallSnapshot(args *InstallSnapshotsArgs, reply *InstallSnapshotsReply){
		DPrintf("Install\n")

		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()

		reply.Term = rf.currentTerm
		//1. Reply immediately if term < currentTerm
		if args.Term < rf.currentTerm{
			return
		}

		rf.lastHeartBeat = time.Now()
		if args.Term > rf.currentTerm{
			rf.ToFollower(args.Term)
		}
		if args.LastIncludedIndex <= rf.lastIncludedIndex {
			return
		}

		if args.LastIncludedIndex < rf.logLen()-1 {
			// the args.LastIncludedIndex log has agreed, if there are more logs, just retain them
			rf.log = append(make([]LogEntry, 0), rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
		} else {
			// 7. discard the entire log
			// empty log use for AppendEntries RPC consistency check
			rf.log = []LogEntry{{nil, args.LastIncludedTerm,}}
		}

		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.snapshot = args.Data

		rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)

		if rf.lastApplied > rf.lastIncludedIndex{
			return
		}

		//8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
		applyMsg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data}
		rf.applyCh <- applyMsg

}

	func (rf *Raft) sendInstallSnapshot(peer int, args InstallSnapshotsArgs, reply InstallSnapshotsReply){
		DPrintf("sendInstall\n")

		ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()


		if rf.serverState != Leader || args.Term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.ToFollower(reply.Term)
			return
		}
		rf.updateMatchIndex(peer, rf.lastIncludedIndex)

}
*/
package raft

import (
	"bytes"

	"6.5840/labgob"
)

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
	send(rf.appendCh)

	// check snapshot may expire by lock competition, otherwise rf.logs may overflow below
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 2. create new snapshot file if first chunk (offset is 0)
	// 3. write data into snapshot file at given offset
	// 4. reply and wait for more data chunks if done is false
	// 5. save snapshot file, discard any existing or partial snapshot with a smaller index

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
	rf.persistStatesAndSnapshot(args.Data)

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

func (rf *Raft) persistStatesAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.Save(data, snapshot)
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(peer int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		LeaderId:          rf.me,
		Data:              rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.serverState != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.ToFollower(reply.Term)
		return
	}

	// update succeeded
	rf.updateMatchIndex(peer, rf.lastIncludedIndex)
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// lock competition may delay snapshot call, check it,
	// otherwise rf.logAt(index) may out of bounds
	if index <= rf.lastIncludedIndex {
		return
	}

	rf.log = append(make([]LogEntry, 0), rf.log[index-rf.lastIncludedIndex:]...)
	// update new lastIncludedIndex and lastIncludedTerm
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLogEntry(index).Term
	// save snapshot
	rf.persistStatesAndSnapshot(snapshot)
}
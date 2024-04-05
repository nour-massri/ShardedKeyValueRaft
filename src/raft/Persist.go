/*
package raft

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)

	rf.persister.Save(buffer.Bytes(), rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// bootstrap without any state?
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Errorf("[readPersist]: Decode Error!\n")
	} else {
		rf.mu.Lock()
		rf.currentTerm, rf.votedFor, rf.log = currentTerm, voteFor, logs
		rf.lastIncludedTerm, rf.lastIncludedIndex = lastIncludedTerm, lastIncludedIndex
		rf.commitIndex, rf.lastApplied = rf.lastIncludedIndex, rf.lastIncludedIndex
		rf.mu.Unlock()
	}
}
*/

package raft

import "sort"

func send(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func (rf *Raft) getPrevLogIdx(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIdx := rf.getPrevLogIdx(i)
	if prevLogIdx < rf.lastIncludedIndex {
		return -1
	}
	return rf.getLogEntry(prevLogIdx).Term
}

func (rf *Raft) getLastLogIdx() int {
	return rf.logLen() - 1
}


func (rf *Raft) updateMatchIndex(server int, matchIdx int) {
	rf.matchIndex[server] = matchIdx
	rf.nextIndex[server] = matchIdx + 1
	rf.updateCommitIndex()
}

func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.logLen() - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.getLogEntry(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.Commit()
	}
}

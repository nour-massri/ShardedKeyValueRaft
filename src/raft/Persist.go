package raft

import (
	"bytes"

	"6.5840/labgob"
)

//save state and snapshot to desk
func (rf *Raft) persist(snapshot []byte) {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)

	rf.persister.Save(buffer.Bytes(), snapshot)
}

//restore state and snapshot from desk
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
	rf.mu.Unlock()
}


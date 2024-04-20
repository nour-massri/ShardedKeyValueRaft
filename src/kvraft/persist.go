package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

//save state and snapshot to desk
func (kv *KVServer) persist() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.stateMachine)
	encoder.Encode(kv.lastClientSerial)
	return buffer.Bytes()
}

//restore state and snapshot from desk
func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	buffer := new(bytes.Buffer)
	decoder := labgob.NewDecoder(buffer)

	kv.mu.Lock()
	decoder.Decode(&kv.stateMachine)
	decoder.Decode(&kv.lastClientSerial)
	kv.mu.Unlock()
}


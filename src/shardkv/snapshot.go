package shardkv

import (
	"bytes"

	"6.5840/labgob"
)

// 检测Raft日志大小
func (kv *ShardKV) checkSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}

	if float64(kv.persister.RaftStateSize())/float64(kv.maxraftstate) > 0.95 {
		go kv.rf.Snapshot(index, kv.persist())
	}
}
func (kv *ShardKV) persist() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	kv.mu.Lock()

	encoder.Encode(kv.stateMachine)
	encoder.Encode(kv.lastClientSerial)
	encoder.Encode(kv.config)
	encoder.Encode(kv.shardsToPull)
	encoder.Encode(kv.shardsToServe)
	encoder.Encode(kv.stateMachineToPush)
	encoder.Encode(kv.garbages)
	kv.mu.Unlock()

	return buffer.Bytes()
}


//restore state and snapshot from desk

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	kv.mu.Lock()
	decoder.Decode(&kv.stateMachine)
	decoder.Decode(&kv.lastClientSerial)
	decoder.Decode(&kv.config)
	decoder.Decode(&kv.shardsToPull)
	decoder.Decode(&kv.shardsToServe)
	decoder.Decode(&kv.stateMachineToPush)
	decoder.Decode(&kv.garbages)
	kv.mu.Unlock()
}
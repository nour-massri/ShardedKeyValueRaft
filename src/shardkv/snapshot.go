// package shardkv

// import (
// 	"bytes"

// 	"6.5840/labgob"
// )

// // 检测Raft日志大小
// func (kv *ShardKV) checkSnapshot(index int) {
// 	if kv.maxraftstate == -1 {
// 		return
// 	}

// 	if float64(kv.persister.RaftStateSize())/float64(kv.maxraftstate) > 0.95 {
// 		go kv.rf.Snapshot(index, kv.persist())
// 	}
// }
// func (kv *ShardKV) persist() []byte {
// 	buffer := new(bytes.Buffer)
// 	encoder := labgob.NewEncoder(buffer)
// 	kv.mu.Lock()

// 	encoder.Encode(kv.stateMachine)
// 	encoder.Encode(kv.lastClientSerial)
// 	encoder.Encode(kv.config)
// 	encoder.Encode(kv.shardsToPull)
// 	encoder.Encode(kv.shardsToServe)
// 	encoder.Encode(kv.stateMachineToPush)
// 	encoder.Encode(kv.garbages)
// 	kv.mu.Unlock()

// 	return buffer.Bytes()
// }

// //restore state and snapshot from desk

// func (kv *ShardKV) readPersist(data []byte) {
// 	if data == nil || len(data) < 1 {
// 		return
// 	}

//		buffer := bytes.NewBuffer(data)
//		decoder := labgob.NewDecoder(buffer)
//		kv.mu.Lock()
//		decoder.Decode(&kv.stateMachine)
//		decoder.Decode(&kv.lastClientSerial)
//		decoder.Decode(&kv.config)
//		decoder.Decode(&kv.shardsToPull)
//		decoder.Decode(&kv.shardsToServe)
//		decoder.Decode(&kv.stateMachineToPush)
//		decoder.Decode(&kv.garbages)
//		kv.mu.Unlock()
//	}
package shardkv

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

func (kv *ShardKV) checkSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}

	if float64(kv.persister.RaftStateSize())/float64(kv.maxraftstate) > 0.95 {
		go kv.rf.Snapshot(index, kv.persist())
	}
}

// 快照数据编码
func (kv *ShardKV) persist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastClientSerial)
	e.Encode(kv.shardsToPull)
	e.Encode(kv.stateMachineToPush)
	e.Encode(kv.shardsToServe)
	e.Encode(kv.config)
	e.Encode(kv.garbages)
	kv.mu.Unlock()
	return w.Bytes()
}

// 快照数据解码
func (kv *ShardKV) readPersist(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2Seq map[int64]int
	var toOutShards map[int]map[int]map[string]string
	var comeInShards map[int]int
	var myShards map[int]bool
	var garbages map[int]map[int]bool
	var cfg shardctrler.Config
	if d.Decode(&db) != nil || d.Decode(&cid2Seq) != nil || d.Decode(&comeInShards) != nil ||
		d.Decode(&toOutShards) != nil || d.Decode(&myShards) != nil || d.Decode(&cfg) != nil ||
		d.Decode(&garbages) != nil {
		fmt.Errorf("[decodeSnapshot]: Decode Error!\n")
	} else {
		kv.stateMachine, kv.lastClientSerial, kv.config = db, cid2Seq, cfg
		kv.stateMachineToPush, kv.shardsToPull, kv.shardsToServe, kv.garbages = toOutShards, comeInShards, myShards, garbages
	}
}
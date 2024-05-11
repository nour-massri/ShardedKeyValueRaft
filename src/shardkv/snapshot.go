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

func (kv *ShardKV) persist() []byte {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	kv.mu.Lock()
	encoder.Encode(kv.stateMachine)
	encoder.Encode(kv.lastClientSerial)
	encoder.Encode(kv.shardsToPull)
	encoder.Encode(kv.shardsToPush)
	encoder.Encode(kv.shardsToServe)
	encoder.Encode(kv.config)
	encoder.Encode(kv.garbages)
	kv.mu.Unlock()
	return w.Bytes()
}

func (kv *ShardKV) readPersist(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	var db map[string]string
	var cid2Seq map[int64]int
	var toOutShards map[int]map[int]map[string]string
	var comeInShards map[int]int
	var myShards map[int]bool
	var garbages map[int]map[int]bool
	var cfg shardctrler.Config
	if decoder.Decode(&db) != nil || decoder.Decode(&cid2Seq) != nil || decoder.Decode(&comeInShards) != nil ||
		decoder.Decode(&toOutShards) != nil || decoder.Decode(&myShards) != nil || decoder.Decode(&cfg) != nil ||
		decoder.Decode(&garbages) != nil {
		fmt.Errorf("[decodeSnapshot]: Decode Error!\n")
	} else {
		kv.stateMachine, kv.lastClientSerial, kv.config = db, cid2Seq, cfg
		kv.shardsToPush, kv.shardsToPull, kv.shardsToServe, kv.garbages = toOutShards, comeInShards, myShards, garbages
	}
}
package shardkv

import (
	"bytes"

	"6.5840/labgob"
	"6.5840/shardctrler"
)



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
	decoder := labgob.NewDecoder(bytes.NewBuffer(snapshot))
	var stateMachine map[string]string
	var lastClientSerial map[int64]int
	var shardsToPull map[int]int
	var shardsToPush map[int]map[int]map[string]string
	var shardsToServe map[int]bool
	var garbages map[int]map[int]bool
	var cfg shardctrler.Config

	if !(decoder.Decode(&stateMachine) != nil || decoder.Decode(&lastClientSerial) != nil || decoder.Decode(&shardsToPull) != nil ||
		decoder.Decode(&shardsToPush) != nil || decoder.Decode(&shardsToServe) != nil || decoder.Decode(&cfg) != nil ||
		decoder.Decode(&garbages) != nil) {
		kv.stateMachine, kv.lastClientSerial, kv.config = stateMachine, lastClientSerial, cfg
		kv.shardsToPush, kv.shardsToPull, kv.shardsToServe, kv.garbages = shardsToPush, shardsToPull, shardsToServe, garbages
	}
}
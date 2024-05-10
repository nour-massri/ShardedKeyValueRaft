package shardkv

import (
	"strconv"
)

func (kv *ShardKV) GarbageCollection(args *GetShardsArgs, reply *GetShardsReply) {
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.toOutShards[args.ConfigNum]; !ok {
		return
	}
	if _, ok := kv.toOutShards[args.ConfigNum][args.Shard]; !ok {
		return
	}
	command := Op{"GC", strconv.Itoa(args.ConfigNum), "", nrand(), args.Shard, nrand()}
	kv.mu.Unlock()
	reply.Err, _ = kv.handle(command)
	kv.mu.Lock()
}

func (kv *ShardKV) gc(cfgNum int, shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.toOutShards[cfgNum]; ok {
		delete(kv.toOutShards[cfgNum], shard)
		if len(kv.toOutShards[cfgNum]) == 0 {
			delete(kv.toOutShards, cfgNum)
		}
	}
}

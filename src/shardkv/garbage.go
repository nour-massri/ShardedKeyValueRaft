package shardkv

import (
	"sync"
	"time"

	"6.5840/shardctrler"
)

func (kv *ShardKV) GarbageCollection(args *GetShardsArgs, reply *GetShardsReply) {
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.shardsToPush[args.ConfigNum]; !ok {
		return
	}
	if _, ok := kv.shardsToPush[args.ConfigNum][args.Shard]; !ok {
		return
	}
	command := Op{
		Type:"Garbage", 
		ConfigNum: args.ConfigNum, 
		Shard: args.Shard,
		Rnd: nrand(),
	}
	kv.mu.Unlock()
	reply.Err, _ = kv.sendCommand(command)
	kv.mu.Lock()
}

func (kv *ShardKV) gc(cfgNum int, shard int) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.shardsToPush[cfgNum]; ok {
		delete(kv.shardsToPush[cfgNum], shard)
		if len(kv.shardsToPush[cfgNum]) == 0 {
			delete(kv.shardsToPush, cfgNum)
		}
	}
	return OK, ""
}


func (kv *ShardKV) tryGC() {
	for {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.garbages) == 0 {
			kv.mu.Unlock()
			time.Sleep(100*time.Millisecond)
			continue
		}
		var wait sync.WaitGroup
		for cfgNum, shards := range kv.garbages {
			for shard := range shards {
				wait.Add(1)
				go func(shard int, cfg shardctrler.Config) {
					defer wait.Done()
					args := GetShardsArgs{shard, cfg.Num}
					gid := cfg.Shards[shard]
					for _, server := range cfg.Groups[gid] {
						srv := kv.make_end(server)
						reply := GetShardsReply{}
						if ok := srv.Call("ShardKV.GarbageCollection", &args, &reply); ok && reply.Err == OK {
							kv.mu.Lock()
							delete(kv.garbages[cfgNum], shard)
							if len(kv.garbages[cfgNum]) == 0 {
								delete(kv.garbages, cfgNum)
							}
							kv.mu.Unlock()
						}
					}
				}(shard, kv.ck.Query(cfgNum))
			}
		}
		kv.mu.Unlock()
		wait.Wait()
		time.Sleep(100*time.Millisecond)
	}
}
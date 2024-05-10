package shardkv

import (
	"strconv"
	"sync"

	"6.5840/shardctrler"
)

func (kv *ShardKV) GarbageCollection(args *GetShardsArgs, reply *GetShardsReply) {
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.stateMachineToPush[args.ConfigNum]; !ok {
		return
	}
	if _, ok := kv.stateMachineToPush[args.ConfigNum][args.Shard]; !ok {
		return
	}
	command := Op{OpType:"GC",Key:strconv.Itoa(args.ConfigNum), Value:"", ClientId: nrand(), Serial: args.Shard, Rnd:nrand()}
	kv.mu.Unlock()
	reply.Err, _ = kv.sendCommand(command)
	kv.mu.Lock()
}

func (kv *ShardKV) tryGC() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.garbages) == 0 {
		kv.mu.Unlock()
		return
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
}

func (kv *ShardKV) gc(index int, op Op, cfgNum int, shard int) {
	kv.mu.Lock()
	if _, ok := kv.stateMachineToPush[cfgNum]; ok {
		delete(kv.stateMachineToPush[cfgNum], shard)
		if len(kv.stateMachineToPush[cfgNum]) == 0 {
			delete(kv.stateMachineToPush, cfgNum)
		}
	}
	if _, ok := kv.commandChannel[index]; !ok {
		kv.commandChannel[index] = make(chan Op, 1)
	}
	ch := kv.commandChannel[index]
	kv.mu.Unlock()

	select {
		case <-ch:
		default:
	}
	ch <- op
}
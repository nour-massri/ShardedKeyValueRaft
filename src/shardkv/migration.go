package shardkv

import (
	"sync"
	"time"

	"6.5840/shardctrler"
)

type GetShardsArgs struct {
	Shard int
	ConfigNum int
}

type GetShardsReply struct {
	Err Err
	StateMachine map[string]string
	LastClientSerial map[int64]int
}

func (kv *ShardKV) tryPullShard() {
	for {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.shardsToPull) == 0 {
			kv.mu.Unlock()
			time.Sleep(80*time.Millisecond)
			continue
		}

		var wait sync.WaitGroup
		for shard, idx := range kv.shardsToPull {
			wait.Add(1)
			go func(shard int, cfg shardctrler.Config) {
				defer wait.Done()
				args := GetShardsArgs{shard, cfg.Num}
				gid := cfg.Shards[shard]
				for _, server := range cfg.Groups[gid] {
					srv := kv.make_end(server)
					reply := GetShardsReply{}
					if ok := srv.Call("ShardKV.ShardMigration", &args, &reply); ok && reply.Err == OK {
						kv.rf.Start(Op{
							Type:"Migration",
							Shard: shard,
							ConfigNum: cfg.Num,
							StateMachine: reply.StateMachine,
							LastClientSerial: reply.LastClientSerial,
						})
					}

				}
			}(shard, kv.ck.Query(idx))
		}
		kv.mu.Unlock()
		wait.Wait()
		time.Sleep(80*time.Millisecond)
	}
}

func (kv *ShardKV) ShardMigration(args *GetShardsArgs, reply *GetShardsReply) {
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if args.ConfigNum >= kv.config.Num {
		return
	}
	reply.Err = OK
	reply.StateMachine, reply.LastClientSerial = kv.copyDBAndDedupMap(args.ConfigNum, args.Shard)
}

func (kv *ShardKV) copyDBAndDedupMap(config int, shard int) (map[string]string, map[int64]int) {
	db := make(map[string]string)
	cid2seq := make(map[int64]int)
	for k, v := range kv.shardsToPush[config][shard] {
		db[k] = v
	}
	for k, v := range kv.lastClientSerial {
		cid2seq[k] = v
	}
	return db, cid2seq
}
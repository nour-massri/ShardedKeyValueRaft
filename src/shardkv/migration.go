package shardkv

import (
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

func (kv *ShardKV) pullShards() {
	for {
		_, isLeader := kv.rf.GetState()
		if !isLeader{
			time.Sleep(80*time.Millisecond)
			continue
		}
		kv.mu.Lock()
		for shard, idx := range kv.shardsToPull {
			go func(shard int, cfg shardctrler.Config) {
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
	reply.StateMachine = make(map[string]string)
	reply.LastClientSerial = make(map[int64]int)
	for k, v := range kv.lastClientSerial {
		reply.LastClientSerial[k] = v
	}
	for k, v := range kv.shardsToPush[args.ConfigNum][args.Shard] {
		reply.StateMachine[k] = v
	}
}

func (kv *ShardKV) pullConfig() {
	for {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.shardsToPull) > 0 {
			kv.mu.Unlock()
			time.Sleep(50*time.Millisecond)
			continue
		}
		nxt := kv.config.Num + 1
		kv.mu.Unlock()
		cfg := kv.ck.Query(nxt)
		if nxt == cfg.Num{
			kv.rf.Start(Op{
				Type: "Config",
				Config: cfg,
			})
		}
		time.Sleep(50*time.Millisecond)
	}
}

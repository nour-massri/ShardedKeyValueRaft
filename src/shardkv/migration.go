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


func (kv *ShardKV) ConfigOp(cfg shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfg.Num <= kv.config.Num {
		return
	}

	oldCfg, toOutShard := kv.config, kv.shardsToServe
	kv.shardsToServe, kv.config = make(map[int]bool), cfg
	for shard, gid := range cfg.Shards {
		if gid != kv.gid {
			continue
		}
		if _, ok := toOutShard[shard]; ok || oldCfg.Num == 0 {
			kv.shardsToServe[shard] = true
			delete(toOutShard, shard)
		} else {
			kv.shardsToPull[shard] = oldCfg.Num
		}
	}
	if len(toOutShard) > 0 {
		kv.shardsToPush[oldCfg.Num] = make(map[int]map[string]string)
		for shard := range toOutShard {
			outDb := make(map[string]string)
			for k, v := range kv.stateMachine {
				if key2shard(k) == shard {
					outDb[k] = v
					delete(kv.stateMachine, k)
				}
			}
			kv.shardsToPush[oldCfg.Num][shard] = outDb
		}
	}
}

func (kv *ShardKV) MigrationOp(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.ConfigNum != kv.config.Num-1 {
		return
	}

	delete(kv.shardsToPull, op.Shard)
	if _, ok := kv.shardsToServe[op.Shard]; !ok {
		kv.shardsToServe[op.Shard] = true
		for k, v := range op.StateMachine {
			kv.stateMachine[k] = v
		}
		for k, v := range op.LastClientSerial {
			kv.lastClientSerial[k] = max(v, kv.lastClientSerial[k])
		}
		if _, ok := kv.garbages[op.ConfigNum]; !ok {
			kv.garbages[op.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[op.ConfigNum][op.Shard] = true
	}
}

func (kv *ShardKV) ClientOp(op Op)(Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.shardsToServe[key2shard(op.Key)]; !ok {
		return ErrWrongGroup, ""
	} else {
		serial, exists := kv.lastClientSerial[op.ClientId]
		if !exists || op.Serial > serial {
			kv.lastClientSerial[op.ClientId] = op.Serial
			if op.Type == "Put" {
				kv.stateMachine[op.Key] = op.Value
			} else if op.Type == "Append" {
				kv.stateMachine[op.Key] += op.Value
			}
		}
		if op.Type == "Get" {
			return OK, kv.stateMachine[op.Key]
		}
		return OK, ""
	}
}


func (kv *ShardKV) applyMsg() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.SnapshotValid {
			kv.readPersist(applyMsg.Snapshot)
			continue
		}
		op := applyMsg.Command.(Op)
		if op.Type == "Config" {
			kv.ConfigOp(op.Config)
		} else if op.Type == "Migration" {
			kv.MigrationOp(op)
		} else {
			var err Err
			var value string
			if op.Type == "Garbage" {
				err, value = kv.gc(op.ConfigNum, op.Shard)
			} else {
				err, value = kv.ClientOp(op)
			}
			kv.mu.Lock()
			var ch chan OpRes
			if _, ok := kv.commandChannel[applyMsg.CommandIndex]; !ok {
				ch = nil
			} else{
			ch = kv.commandChannel[applyMsg.CommandIndex]}
			kv.mu.Unlock()

			if ch != nil{
				select {
				case <-ch:
				default:
				}
				ch <- OpRes{err, value, op.Rnd}
			}
		}
		kv.checkSnapshot(applyMsg.CommandIndex)
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

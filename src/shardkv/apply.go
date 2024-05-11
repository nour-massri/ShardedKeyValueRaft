package shardkv

import "6.5840/shardctrler"


func (kv *ShardKV) ConfigOp(newConfig shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if newConfig.Num <= kv.config.Num {
		return
	}

	oldConfig, tmpShard := kv.config, kv.shardsToServe
	kv.shardsToServe = make(map[int]bool)
	kv.config = newConfig
	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			_, ok := tmpShard[shard]
			if  ok || oldConfig.Num == 0 {
				delete(tmpShard, shard)
				kv.shardsToServe[shard] = true
			} else {
				kv.shardsToPull[shard] = oldConfig.Num
			}
		}
	}
	
	kv.shardsToPush[oldConfig.Num] = make(map[int]map[string]string)
	for shard := range tmpShard {
		kv.shardsToPush[oldConfig.Num][shard] = make(map[string]string)
		
		for k, v := range kv.stateMachine {
			if key2shard(k) == shard {
				kv.shardsToPush[oldConfig.Num][shard][k] = v
				delete(kv.stateMachine, k)
			}
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
	if _, ok := kv.shardsToServe[op.Shard]; ok {
		return 
	}
	for k, v := range op.LastClientSerial {
		kv.lastClientSerial[k] = max(v, kv.lastClientSerial[k])
	}
	for k, v := range op.StateMachine {
		kv.stateMachine[k] = v
	}
	kv.shardsToServe[op.Shard] = true

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

				err, value = kv.ClientOp(op)
			
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
		if kv.maxraftstate != -1 && 10* kv.persister.RaftStateSize() > 9 * kv.maxraftstate {
			go kv.rf.Snapshot(applyMsg.CommandIndex, kv.persist())
		}
	}
}
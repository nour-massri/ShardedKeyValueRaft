package shardkv

type GetShardsArgs struct {
	Shard int
	ConfigNum int
}

type GetShardsReply struct {
	Err Err
	DB map[string]string
	Cid2Seq map[int64]int
	Shard int
	ConfigNum int
}


func (kv *ShardKV) ShardMigration(args *GetShardsArgs, reply *GetShardsReply) {
	reply.Err, reply.Shard, reply.ConfigNum = ErrWrongLeader, args.Shard, args.ConfigNum
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if args.ConfigNum >= kv.cfg.Num {
		return
	}
	reply.Err, reply.ConfigNum, reply.Shard = OK, args.ConfigNum, args.Shard
	reply.DB, reply.Cid2Seq = kv.copyDBAndDedupMap(args.ConfigNum, args.Shard)
}

func (kv *ShardKV) copyDBAndDedupMap(config int, shard int) (map[string]string, map[int64]int) {
	db := make(map[string]string)
	cid2seq := make(map[int64]int)
	for k, v := range kv.toOutShards[config][shard] {
		db[k] = v
	}
	for k, v := range kv.cid2seq {
		cid2seq[k] = v
	}
	return db, cid2seq
}
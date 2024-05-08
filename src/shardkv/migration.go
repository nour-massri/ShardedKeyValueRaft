package shardkv

type GetShardsArgs struct {
	Shard int
	ConfigNum int
}

type GetShardsReply struct {
	Err Err
	StateMachine map[string]string
	LastClientSerial map[int64]int
}

func (kv *ShardKV) sendGetShards(shard int, configNum int){
	cfg := kv.mck.Query(configNum)
	gid := cfg.Shards[shard]
	args := GetShardsArgs{Shard: shard, ConfigNum: configNum}
	for _, serverName := range cfg.Groups[gid]{
		server := kv.make_end(serverName)
		reply := GetShardsReply{}

		ok := server.Call("ShardKV.GetShards", &args, &reply)
		if ok && reply.Err == OK{
			//fmt.Printf("got shards: %v %v", shard, configNum)
			kv.rf.Start(
			Op{
				OpType: "Migration",
				ConfigNum: configNum,
				Shard: shard,
			StateMachine: reply.StateMachine, 
			LastClientSerial: reply.LastClientSerial,
		})
		}
	}
}

func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply){
	// _, isLeader := kv.rf.GetState()
	// if !isLeader{
	// 	reply.Err = ErrWrongLeader
	// 	return 
	// }
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	// fmt.Printf("got shards: %v %v\n", args.Shard, args.ConfigNum)

	// v2, ok2 := kv.stateMachineToPush[args.ConfigNum][args.Shard]
	// if !ok2{
	// 	reply.Err = ErrWrongGroup
	// 	return 
	// }
	// reply.StateMachine = v2

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
	for k, v := range kv.stateMachineToPush[config][shard] {
		db[k] = v
	}
	for k, v := range kv.lastClientSerial {
		cid2seq[k] = v
	}
	return db, cid2seq
}

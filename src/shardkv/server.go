package shardkv

import (
	"strconv"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	Type  string
	Key   string
	Value string
	ClientId   int64
	Serial   int
	Rnd int64
}

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ck       *shardctrler.Clerk
	config       shardctrler.Config
	persister *raft.Persister

	stateMachine        map[string]string
	commandChannel    map[int]chan Op
	lastClientSerial   map[int64]int

	shardsToPush  map[int]map[int]map[string]string "cfg number -> (shard -> db)"
	shardsToPull map[int]int                       "shard -> config number"
	shardsToServe       map[int]bool                      "record which i-shard can offer service"
	garbages     map[int]map[int]bool              "cfg number -> shards"

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	command := Op{"Get", args.Key, "", nrand(), 0, nrand()}
	reply.Err, reply.Value = kv.handle(command)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op{args.Op, args.Key, args.Value, args.ClientId, args.Serial, nrand()}
	reply.Err, _ = kv.handle(command)
}

func (kv *ShardKV) handle(command Op) (Err, string) {
	index, _, isLeader := kv.rf.Start(command)
	if isLeader {
		ch := kv.put(index, true)
		op := kv.notified(ch, index)
		if op.Type == command.Type && op.Rnd == command.Rnd {
			return OK, op.Value
		}
		if op.Type == ErrWrongGroup {
			return ErrWrongGroup, ""
		}
	}
	return ErrWrongLeader, ""
}

func send(notifyCh chan Op, op Op) {
	select {
	case <-notifyCh:
	default:
	}
	notifyCh <- op
}

func (kv *ShardKV) notified(ch chan Op, index int) Op {
	select {
	case notifyArg, ok := <-ch:
		if ok {
			close(ch)
		}
		kv.mu.Lock()
		delete(kv.commandChannel, index)
		kv.mu.Unlock()
		return notifyArg
	case <-time.After(time.Duration(1000) * time.Millisecond):
		return Op{}
	}
}



func (kv *ShardKV) put(idx int, createIfNotExists bool) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.commandChannel[idx]; !ok {
		if !createIfNotExists {
			return nil
		}
		kv.commandChannel[idx] = make(chan Op, 1)
	}
	return kv.commandChannel[idx]
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
}

func (kv *ShardKV) tryPollNewCfg() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.shardsToPull) > 0 {
		kv.mu.Unlock()
		return
	}
	next := kv.config.Num + 1
	kv.mu.Unlock()
	cfg := kv.ck.Query(next)
	if cfg.Num == next {
		kv.rf.Start(cfg) //sync follower with new cfg
	}
}


func (kv *ShardKV) tryPullShard() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.shardsToPull) == 0 {
		kv.mu.Unlock()
		return
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
					kv.rf.Start(reply)
				}

			}
		}(shard, kv.ck.Query(idx))
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) daemon(do func(), sleepMS int) {
	for {
		select {
		default:
			do()
		}
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}
}

func (kv *ShardKV) apply(applyMsg raft.ApplyMsg) {
	if cfg, ok := applyMsg.Command.(shardctrler.Config); ok {
		kv.updateInAndOutDataShard(cfg)
	} else if migrationData, ok := applyMsg.Command.(GetShardsReply); ok {
		kv.updateDBWithMigrateData(migrationData)
	} else {
		op := applyMsg.Command.(Op)
		if op.Type == "GC" {
			cfgNum, _ := strconv.Atoi(op.Key)
			kv.gc(cfgNum, op.Serial)
		} else {
			kv.normal(&op)
		}
		if notifyCh := kv.put(applyMsg.CommandIndex, false); notifyCh != nil {
			send(notifyCh, op)
		}
	}
	kv.checkSnapshot(applyMsg.CommandIndex)
}

func (kv *ShardKV) updateInAndOutDataShard(cfg shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfg.Num <= kv.config.Num { //only consider newer config
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
	if len(toOutShard) > 0 { // prepare data that needed migration
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

func (kv *ShardKV) updateDBWithMigrateData(migrationData GetShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if migrationData.ConfigNum != kv.config.Num-1 {
		return
	}

	delete(kv.shardsToPull, migrationData.Shard)
	//this check is necessary, to avoid use  kv.cfg.Num-1 to update kv.cfg.Num's shard
	if _, ok := kv.shardsToServe[migrationData.Shard]; !ok {
		kv.shardsToServe[migrationData.Shard] = true
		for k, v := range migrationData.StateMachine {
			kv.stateMachine[k] = v
		}
		for k, v := range migrationData.LastClientSerial {
			kv.lastClientSerial[k] = max(v, kv.lastClientSerial[k])
		}
		if _, ok := kv.garbages[migrationData.ConfigNum]; !ok {
			kv.garbages[migrationData.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[migrationData.ConfigNum][migrationData.Shard] = true
	}
}

func (kv *ShardKV) normal(op *Op) {
	shard := key2shard(op.Key)
	kv.mu.Lock()
	if _, ok := kv.shardsToServe[shard]; !ok {
		op.Type = ErrWrongGroup
	} else {
		maxSeq, found := kv.lastClientSerial[op.ClientId]
		if !found || op.Serial > maxSeq {
			if op.Type == "Put" {
				kv.stateMachine[op.Key] = op.Value
			} else if op.Type == "Append" {
				kv.stateMachine[op.Key] += op.Value
			}
			kv.lastClientSerial[op.ClientId] = op.Serial
		}
		if op.Type == "Get" {
			op.Value = kv.stateMachine[op.Key]
		}
	}
	kv.mu.Unlock()
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetShardsArgs{})
	labgob.Register(GetShardsReply{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persister = persister

	// Use something like this to talk to the shardmaster:
	kv.ck = shardctrler.MakeClerk(kv.masters)
	kv.config = shardctrler.Config{}

	kv.stateMachine = make(map[string]string)
	kv.commandChannel = make(map[int]chan Op)
	kv.lastClientSerial = make(map[int64]int)

	kv.shardsToPush = make(map[int]map[int]map[string]string)
	kv.shardsToPull = make(map[int]int)
	kv.shardsToServe = make(map[int]bool)
	kv.garbages = make(map[int]map[int]bool)

	kv.readPersist(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.daemon(kv.tryPollNewCfg, 50)
	go kv.daemon(kv.tryPullShard, 80)
	go kv.daemon(kv.tryGC, 100)

	go kv.applyDaemon()
	return kv
}

func (kv *ShardKV) applyDaemon() {
	for {
		
		applyMsg := <-kv.applyCh
		if !applyMsg.CommandValid {
			kv.readPersist(applyMsg.Snapshot)
			continue
		}
		kv.apply(applyMsg)
		
	}
}
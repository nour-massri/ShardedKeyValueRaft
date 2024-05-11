package shardkv

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	Type  string

	//for ClientOp
	Key   string
	Value string
	ClientId   int64
	Serial   int
	Rnd int64

	//config
	Config shardctrler.Config

	//Migration and GC
	Shard int
	ConfigNum int
	StateMachine map[string]string
	LastClientSerial map[int64]int

}

type OpRes struct{
	Err Err
	Value string
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
	commandChannel    map[int]chan OpRes
	lastClientSerial   map[int64]int

	shardsToPush  map[int]map[int]map[string]string
	shardsToPull map[int]int                      
	shardsToServe       map[int]bool
	garbages     map[int]map[int]bool            

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.Err, reply.Value = kv.sendCommand(
		Op{
			Type:"Get", 
			Key:args.Key, 
			Rnd: nrand(),
		},
	)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err, _ = kv.sendCommand(
		Op{
			Type: args.Op, 
			Key: args.Key, 
			Value: args.Value,
			ClientId: args.ClientId,
			Serial: args.Serial,
			Rnd: nrand(),
		},
	)
}

func (kv *ShardKV) sendCommand(command Op) (Err, string) {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return ErrWrongLeader, ""
	}
	kv.mu.Lock()
	if _, ok := kv.commandChannel[index]; !ok {
		kv.commandChannel[index] = make(chan OpRes, 1)
	}
	ch := kv.commandChannel[index]
	kv.mu.Unlock()
	
	opRes := kv.notified(ch, index)
	if opRes.Err == OK && opRes.Rnd == command.Rnd {
		return OK, opRes.Value
	}
	if opRes.Err == ErrWrongGroup {
		return ErrWrongGroup, ""
	}
	return ErrWrongLeader, ""
}

func send(notifyCh chan OpRes, op OpRes) {
	select {
	case <-notifyCh:
	default:
	}
	notifyCh <- op
}

func (kv *ShardKV) notified(ch chan OpRes, index int) OpRes {
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
		return OpRes{}
	}
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
}

func (kv *ShardKV) tryPollNewCfg() {
	for {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.shardsToPull) > 0 {
			kv.mu.Unlock()
			time.Sleep(50*time.Millisecond)
			continue
		}
		next := kv.config.Num + 1
		kv.mu.Unlock()
		cfg := kv.ck.Query(next)
		if cfg.Num == next {
			kv.rf.Start(Op{
				Type: "Config",
				Config: cfg,
			}) //sync follower with new cfg
		}
		time.Sleep(50*time.Millisecond)
	}
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

func (kv *ShardKV) updateDBWithMigrateData(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.ConfigNum != kv.config.Num-1 {
		return
	}

	delete(kv.shardsToPull, op.Shard)
	//this check is necessary, to avoid use  kv.cfg.Num-1 to update kv.cfg.Num's shard
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

func (kv *ShardKV) normal(op Op)(Err, string) {
	shard := key2shard(op.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.shardsToServe[shard]; !ok {
		return ErrWrongGroup, ""
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
			kv.updateInAndOutDataShard(op.Config)
		} else if op.Type == "Migration" {
			kv.updateDBWithMigrateData(op)
		} else {
			var err Err
			var value string
			if op.Type == "GC" {
				err, value = kv.gc(op.ConfigNum, op.Shard)
			} else {
				err, value = kv.normal(op)
			}
			kv.mu.Lock()
			var ch chan OpRes
			if _, ok := kv.commandChannel[applyMsg.CommandIndex]; !ok {
				ch = nil
			} else{
			ch = kv.commandChannel[applyMsg.CommandIndex]}
			kv.mu.Unlock()

			if ch != nil{
				send(ch, OpRes{err, value, op.Rnd})
			}
			
		}
		kv.checkSnapshot(applyMsg.CommandIndex)
	}
}
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {

	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persister = persister
	kv.ck = shardctrler.MakeClerk(kv.masters)
	kv.config = shardctrler.Config{}

	kv.stateMachine = make(map[string]string)
	kv.commandChannel = make(map[int]chan OpRes)
	kv.lastClientSerial = make(map[int64]int)

	kv.shardsToPush = make(map[int]map[int]map[string]string)
	kv.shardsToPull = make(map[int]int)
	kv.shardsToServe = make(map[int]bool)
	kv.garbages = make(map[int]map[int]bool)

	kv.readPersist(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.tryPollNewCfg()
	go kv.tryPullShard()
	go kv.tryGC()
	go kv.applyMsg()

	return kv
}

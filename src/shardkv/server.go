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
	maxraftstate int

	//clients of other services
	ck       *shardctrler.Clerk
	persister *raft.Persister

	//state
	stateMachine        map[string]string
	commandChannel    map[int]chan OpRes
	lastClientSerial   map[int64]int

	//shards and migration
	config       shardctrler.Config
	shardsToPush  map[int]map[int]map[string]string
	shardsToPull map[int]int                      
	shardsToServe       map[int]bool
	garbages     map[int]map[int]bool            

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

	var opRes OpRes
	select {
		case notifyArg, ok := <-ch:
			if ok {
				close(ch)
			}
			kv.mu.Lock()
			delete(kv.commandChannel, index)
			kv.mu.Unlock()
			opRes = notifyArg
		case <-time.After(time.Duration(1000) * time.Millisecond):
			opRes = OpRes{}
	}
	if opRes.Err == OK && opRes.Rnd == command.Rnd {
		return OK, opRes.Value
	}
	if opRes.Err == ErrWrongGroup {
		return ErrWrongGroup, ""
	}
	return ErrWrongLeader, ""
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

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
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
		if kv.maxraftstate != -1 && 10* kv.persister.RaftStateSize() > 9 * kv.maxraftstate {
			go kv.rf.Snapshot(applyMsg.CommandIndex, kv.persist())
		}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ck = shardctrler.MakeClerk(kv.masters)
	kv.persister = persister

	kv.stateMachine = make(map[string]string)
	kv.commandChannel = make(map[int]chan OpRes)
	kv.lastClientSerial = make(map[int64]int)

	kv.config = shardctrler.Config{}
	kv.shardsToPush = make(map[int]map[int]map[string]string)
	kv.shardsToPull = make(map[int]int)
	kv.shardsToServe = make(map[int]bool)
	kv.garbages = make(map[int]map[int]bool)

	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.pullConfig()
	go kv.tryPullShard()
	go kv.tryGC()
	go kv.applyMsg()

	return kv
}

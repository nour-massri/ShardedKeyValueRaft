package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key   string
	Value string
	ClientId int64
	Serial int

	Rnd int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config   shardctrler.Config

	stateMachine map[string]string
	lastClientSerial map[int64]int
	commandChannel map[int]chan Op

	mck *shardctrler.Clerk
	persister *raft.Persister

}

func (kv *ShardKV) sendCommand(op Op) Err{
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		return ErrWrongLeader
	}
	kv.mu.Lock()
	if _, ok := kv.commandChannel[index]; !ok {
		kv.commandChannel[index] = make(chan Op, 1)
	}
	ch := kv.commandChannel[index]
	kv.mu.Unlock()
	var res Op
	select {
	case res = <-ch:
	case <-time.After(time.Duration(700) * time.Millisecond):
		res =  Op{}
	}
	if res.Rnd != op.Rnd{
		return ErrWrongLeader
	}
	return OK
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType: "Get",
		Key: args.Key,
		ClientId: 0,
		Serial: 0,
		Rnd: nrand(),

	}
	err := kv.sendCommand(op)
	reply.Err = err
	if err == OK{
		kv.mu.Lock()
		kv.config = kv.mck.Query(-1)
		shard := key2shard(args.Key)
		if kv.config.Shards[shard] != kv.gid{
			reply.Err = ErrWrongGroup
		} else {
			reply.Value = kv.stateMachine[args.Key]
		}
		kv.mu.Unlock()
	} else {
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		Serial: args.Serial,
		Rnd: nrand(),
	}
	err := kv.sendCommand(op)
	reply.Err = err

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) applyTicker() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.SnapshotValid{
			kv.readPersist(applyMsg.Snapshot)
			continue
		}
		op := applyMsg.Command.(Op)
		kv.mu.Lock()
		lastSerial, exists := kv.lastClientSerial[op.ClientId]
		if !exists || lastSerial < op.Serial{
			if op.OpType == "Get"{
			} else if op.OpType == "Put"{
				kv.stateMachine[op.Key] = op.Value
			} else if op.OpType == "Append"{
				kv.stateMachine[op.Key] += op.Value
			}
			kv.lastClientSerial[op.ClientId] = op.Serial
		}
	
		if _, ok := kv.commandChannel[applyMsg.CommandIndex]; !ok {
			kv.commandChannel[applyMsg.CommandIndex] = make(chan Op, 1)
		}
		ch := kv.commandChannel[applyMsg.CommandIndex]
		kv.mu.Unlock()

		if kv.maxraftstate != -1 && kv.persister.RaftStateSize()  > kv.maxraftstate/2 {
			go kv.rf.Snapshot(applyMsg.CommandIndex, kv.persist())
		}
		select {
			case <-ch:
			default:
		}
		ch <- op
	}
}
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister
	kv.stateMachine = make(map[string]string)
	kv.lastClientSerial = make(map[int64]int)
	kv.commandChannel  = make(map[int]chan Op)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	//from this you can tell that the controlers replica group is an indepdent group
	//and the shardedkv are clients(make clerk) to this controlers group

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readPersist(kv.persister.ReadSnapshot())
	go kv.applyTicker()


	return kv
}
func (kv *ShardKV) persist() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.stateMachine)
	encoder.Encode(kv.lastClientSerial)
	return buffer.Bytes()
}


//restore state and snapshot from desk

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	kv.mu.Lock()
	decoder.Decode(&kv.stateMachine)
	decoder.Decode(&kv.lastClientSerial)
	kv.mu.Unlock()
}
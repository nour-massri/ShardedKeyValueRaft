package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	persister *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine map[string]string
	lastClientSerial map[int64]int
	commandChannel map[int]chan Op
}

func (kv *KVServer) sendCommand(op Op) Err{
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType: "Get",
		Key: args.Key,
		ClientId: 0,
		Serial: 0,
		Rnd: nrand(),

	}
	err := kv.sendCommand(op)
	if err == OK{
		kv.mu.Lock()
		reply.Value = kv.stateMachine[args.Key]
		kv.mu.Unlock()
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType: "Put",
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		Serial: args.Serial,
		Rnd: nrand(),
	}
	err := kv.sendCommand(op)
	reply.Err = err

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType: "Append",
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		Serial: args.Serial,
		Rnd: nrand(),

	}
	err := kv.sendCommand(op)
	reply.Err = err
}
 
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) applyTicker() {
	for !kv.killed(){
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

		if kv.maxraftstate == -1 || kv.persister.RaftStateSize()  > kv.maxraftstate/2 {
			go kv.rf.Snapshot(applyMsg.CommandIndex, kv.persist())
		}
		select {
			case <-ch:
			default:
		}
		ch <- op
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	
	kv.stateMachine = make(map[string]string)
	kv.lastClientSerial = make(map[int64]int)
	kv.commandChannel  = make(map[int]chan Op)

	kv.readPersist(kv.persister.ReadSnapshot())
	go kv.applyTicker()

	// You may need initialization code here.

	return kv
}

//save state and snapshot to desk

func (kv *KVServer) persist() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.stateMachine)
	encoder.Encode(kv.lastClientSerial)
	return buffer.Bytes()
}


//restore state and snapshot from desk

func (kv *KVServer) readPersist(data []byte) {
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
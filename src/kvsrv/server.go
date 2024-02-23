package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	mp map[string]string
	results map[int64]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.mp[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.results[args.RequestId] 
	if !ok  {
	 	kv.mp[args.Key] = args.Value
		kv.results[args.RequestId] = value
	}else{
		reply.Value = value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.results[args.RequestId] 
	if !ok  {
		reply.Value = kv.mp[args.Key]
		kv.results[args.RequestId] = kv.mp[args.Key]
	 	kv.mp[args.Key] += args.Value
	}else{
		reply.Value = value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mp = make(map[string]string)
	kv.results = make(map[int64]string)
	return kv
}

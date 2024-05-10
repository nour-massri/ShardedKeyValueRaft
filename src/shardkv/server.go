package shardkv

//channel and rpc sending requests, on both sides do checks

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
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string //"Get", "Put", "Append", "Config", "Migration"

	//for put append and get
	Key   string
	Value string
	ClientId int64
	Serial int
	Rnd int64//for get, append and put equal check

	//for change config
	Config shardctrler.Config

	//for update state
	Shard int
	ConfigNum int
	StateMachine map[string]string
	LastClientSerial map[int64]int

	//return values of op executions
	Err Err
	Ret string


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

	stateMachine map[string]string
	lastClientSerial map[int64]int
	commandChannel map[int]chan Op

	shardsToPull map[int]int
	shardsToServe map[int]bool
	//to save state at specific configNum time
	stateMachineToPush map[int]map[int]map[string]string

	garbages     map[int]map[int]bool              "cfg number -> shards"


	config   shardctrler.Config
	ck *shardctrler.Clerk
	persister *raft.Persister

	killCh chan bool
}

func (kv *ShardKV) sendCommand(op Op) (Err, string){
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		return ErrWrongLeader, ""
	}
	kv.mu.Lock()
	if _, ok := kv.commandChannel[index]; !ok {
		kv.commandChannel[index] = make(chan Op, 1)
	}
	ch := kv.commandChannel[index]
	kv.mu.Unlock()
	var res Op
	var ok bool
	select {
	case res, ok = <-ch:	
		if ok{
			close(ch)
		}
		kv.mu.Lock()
		delete(kv.commandChannel,index)
		kv.mu.Unlock()
	case <-time.After(time.Duration(1000) * time.Millisecond):
		res =  Op{}
	}
	//get another op type having err and rnd 
	if res.Err == OK && res.Rnd == op.Rnd{
		return  OK, res.Ret
	}
	if res.Err != OK{
		return res.Err, res.Ret
	}
	return ErrWrongLeader, ""//if res.rnd != op.rend means 
	//the leader changed between sending the op at index and commiting op at index 
	//rnd not equal means the other leader committed a different op at index
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType: "Get",
		Key: args.Key,
		ClientId: nrand(),
		Serial: 0,
		Rnd: nrand(),

	}
	reply.Err, reply.Value = kv.sendCommand(op)
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
	reply.Err, _ = kv.sendCommand(op)
	
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	select {
	case <-kv.killCh:
	default:
	}
	kv.killCh <- true
}

// func (kv *ShardKV) applyConfigOp(op Op) {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	newCfg := op.Config
// 	//fmt.Printf("%v %v\n", newCfg.Num, newCfg.Shards)
// 	//update config and shardsToPull in server state and make a slice of 
// 	for shard, newGID := range(newCfg.Shards){
// 		oldGID := kv.config.Shards[shard]
// 		if oldGID == newGID{
// 			continue
// 		}
// 		if newGID == kv.gid{
// 			//shardToPull
// 			kv.shardsToPull[shard] = kv.config.Num //afterbeing pulled shardsToServe[shard] = true
// 		}
// 		if oldGID == kv.gid{
// 			//shardToPush
// 			kv.shardsToServe[shard] = false
// 			//prepare output in kv.statemachinetopush
// 		}
// 	}
// 	kv.config = op.Config

// }

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
		kv.stateMachineToPush[oldCfg.Num] = make(map[int]map[string]string)
		for shard := range toOutShard {
			outDb := make(map[string]string)
			for k, v := range kv.stateMachine {
				if key2shard(k) == shard {
					outDb[k] = v
					delete(kv.stateMachine, k)
				}
			}
			kv.stateMachineToPush[oldCfg.Num][shard] = outDb
		}
	}
}

func (kv *ShardKV) applyMigrationOp(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.ConfigNum != kv.config.Num-1 {
		return
	}
	delete(kv.shardsToPull, op.Shard)
	if _, ok := kv.shardsToServe[op.Shard]; ok {
		return 
	}	
	kv.shardsToServe[op.Shard] = true
	for k,v := range(op.LastClientSerial){
		oldVal, ok := kv.lastClientSerial[k] 
		if !ok || oldVal < v{
			kv.lastClientSerial[k] = v
		}
	}
	for k,v := range(op.StateMachine){
		kv.stateMachine[k] = v
	}
	if _, ok := kv.garbages[op.ConfigNum]; !ok {
		kv.garbages[op.ConfigNum] = make(map[int]bool)
	}
	kv.garbages[op.ConfigNum][op.Shard] = true
}

func (kv *ShardKV) applyClientOp(index int, op Op){
	//normal operations apply
	kv.mu.Lock()
	if _,ok := kv.shardsToServe[key2shard(op.Key)]; !ok{
		op.Err = ErrWrongGroup
	} else {
		op.Err = OK
		lastSerial, exists := kv.lastClientSerial[op.ClientId]
		if !exists || lastSerial < op.Serial{
			if op.OpType == "Put"{
				kv.stateMachine[op.Key] = op.Value
			} else if op.OpType == "Append"{
				kv.stateMachine[op.Key] += op.Value
			}
			kv.lastClientSerial[op.ClientId] = op.Serial
		}
		if op.OpType == "Get"{
			op.Ret = kv.stateMachine[op.Key]
		}
	}
	if _, ok := kv.commandChannel[index]; !ok {
		kv.commandChannel[index] = make(chan Op, 1)
	}
	ch := kv.commandChannel[index]
	kv.mu.Unlock()

	select {
		case <-ch:
		default:
	}
	ch <- op
}

func (kv *ShardKV) applyTicker() {
	for {
		
		applyMsg := <-kv.applyCh
		if applyMsg.SnapshotValid{
			kv.readPersist(applyMsg.Snapshot)
			continue
		}
		op := applyMsg.Command.(Op)
		if op.OpType == "Config"{
			//kv.applyConfigOp(op)
			kv.updateInAndOutDataShard(op.Config)
		} else if(op.OpType == "Migration"){
			kv.applyMigrationOp(op)
		} else if op.OpType == "GC"{
			cfgNum, _ := strconv.Atoi(op.Key)
			kv.gc(applyMsg.CommandIndex, op, cfgNum, op.Serial)
		} else {
			kv.applyClientOp(applyMsg.CommandIndex, op)

		}
		kv.checkSnapshot(applyMsg.CommandIndex)
	}
}

func (kv *ShardKV) pullConfig() {
	for {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if !isLeader || len(kv.shardsToPull) > 0{
			kv.mu.Unlock()
			time.Sleep(50*time.Millisecond)
			continue
		}
		nxt := kv.config.Num + 1
		kv.mu.Unlock()
		newConfig := kv.ck.Query(nxt)
		if newConfig.Num == nxt{
		kv.rf.Start(Op{OpType:"Config", Config: newConfig})
		}
		time.Sleep(50*time.Millisecond)
	}
}
func (kv *ShardKV) pullShards() {
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
					kv.rf.Start(
						Op{
							OpType:"Migration",
							ConfigNum: reply.ConfigNum,
							Shard: reply.Shard,
							StateMachine: reply.StateMachine,
							LastClientSerial: reply.LastClientSerial,
						})
				}
			}
		}(shard, kv.ck.Query(idx))
	}
	kv.mu.Unlock()
	wait.Wait()
}
// func (kv *ShardKV) pullShards() {
// 	for {
// 		_, isLeader := kv.rf.GetState()
// 		kv.mu.Lock()
// 		if !isLeader || len(kv.shardsToPull) == 0{		
// 			kv.mu.Unlock()
// 			time.Sleep(100*time.Millisecond)
// 			continue
// 		}
// 		var wait sync.WaitGroup
// 		for shard, configNum := range(kv.shardsToPull){
// 			//fmt.Printf("%v %v\n", shard,configNum)
// 			wait.Add(1)
// 			go func(shard int, configNum int, cfg shardctrler.Config){
// 				defer wait.Done()
// 				kv.sendGetShards(shard, configNum, cfg)
// 			}(shard, configNum, kv.ck.Query(configNum))
// 		}
// 		kv.mu.Unlock()
// 		wait.Wait()
// 		time.Sleep(80*time.Millisecond)
// 	} 
// }
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
	kv.ck = shardctrler.MakeClerk(kv.ctrlers)
	//from this you can tell that the controlers replica group is an indepdent group
	//and the shardedkv are clients(make clerk) to this controlers group
	kv.config = shardctrler.Config{}
	kv.shardsToPull = make(map[int]int)
	kv.shardsToServe = make(map[int]bool)
	kv.stateMachineToPush = make(map[int]map[int]map[string]string)

	kv.garbages = make(map[int]map[int]bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool, 1)

	//kv.config make a dummpy one with num = -1 and shards values all -1

	kv.readPersist(kv.persister.ReadSnapshot())
	go kv.applyTicker()
	go kv.pullConfig()
	go kv.daemon(kv.pullShards,80)
	go kv.daemon(kv.tryGC, 100)

	return kv
}
func (kv *ShardKV) daemon(do func(), sleepMS int) {
	for {
		select {
		case <-kv.killCh:
			return
		default:
			do()
		}
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}
}

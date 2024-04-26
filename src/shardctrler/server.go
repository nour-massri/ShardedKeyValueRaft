package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	//for join
	Servers map[int][]string // new GID -> servers mappings
	//for leave
	GIDs []int
	//for move
	Shard int
	GID   int
	//Num
	Num int

	ClientId int
	Serial int

	Rnd int
}
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	shutdown chan struct{} // shutdown chan

	configs []Config // indexed by config num
	configIdx int

	lastClientSerial map[int]int
	commandChannel map[int]chan Op
}	

func (sc *ShardCtrler) sendCommand(op Op) Err{
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader{
		return ErrWrongLeader
	}
	sc.mu.Lock()
	if _, ok := sc.commandChannel[index]; !ok {
		sc.commandChannel[index] = make(chan Op, 1)
	}
	ch := sc.commandChannel[index]
	sc.mu.Unlock()
	var res Op
	select {
	case res = <-ch:
		close(ch)
		sc.mu.Lock()
		delete(sc.commandChannel, index)
		sc.mu.Unlock()
	case <-time.After(time.Duration(700) * time.Millisecond):
		res =  Op{}
	}
	if res.Rnd != op.Rnd{
		return ErrWrongLeader
	}
	return OK

}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpType: "Join",
		Servers: args.Servers,
		ClientId: args.ClientId,
		Serial: args.Serial,
		Rnd: int(nrand()),

	}
	err := sc.sendCommand(op)
	reply.Err = err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpType: "Leave",
		GIDs: args.GIDs,
		ClientId: args.ClientId,
		Serial: args.Serial,
		Rnd: int(nrand()),
	}
	err := sc.sendCommand(op)
	reply.Err = err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpType: "Move",
		Shard: args.Shard,
		GID: args.GID,
		ClientId: args.ClientId,
		Serial: args.Serial,
		Rnd: int(nrand()),
	}
	err := sc.sendCommand(op)
	reply.Err = err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpType: "Get",
		Num: args.Num,
		ClientId: 0,
		Serial: -1,
		Rnd: int(nrand()),
	}
	err := sc.sendCommand(op)
	if err == OK{
		sc.mu.Lock()
		queryIdx := args.Num
		if queryIdx == -1 || queryIdx > sc.configIdx{
			queryIdx = sc.configIdx
		}
		reply.Config = sc.configs[queryIdx]
		sc.mu.Unlock()
	} else {
		reply.Err = err
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.shutdown)

}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) makeNewConfig(){
	sc.configIdx ++;
	sc.configs = append(sc.configs, Config{
		Num: sc.configIdx,
		Groups: make(map[int][]string),
		Shards: sc.configs[sc.configIdx-1].Shards ,
	})
	for key, value := range(sc.configs[sc.configIdx-1].Groups){
		sc.configs[sc.configIdx].Groups[key] = value
	}
}

func (sc *ShardCtrler) groupByGID() map[int][]int{
	cfg  := &sc.configs[sc.configIdx]

	groupShards := map[int][]int{}
	for k, _ := range cfg.Groups {
		groupShards[k] = []int{}
	}
	for k, v := range cfg.Shards {
		groupShards[v] = append(groupShards[v], k)
	}
	return groupShards
}

func (sm *ShardCtrler) getMaxGID(groupShards map[int][]int) int {
	max := -1
	gid := 0

  KeyLenPairs := make([][2]interface{}, 0)

  for key, value := range groupShards {
	KeyLenPairs = append(KeyLenPairs, [2]interface{}{key, len(value)})
  }

  sort.Slice(KeyLenPairs, func(i, j int) bool {
	  return KeyLenPairs[i][0].(int) < KeyLenPairs[j][0].(int)
  })
	for _, pair  := range KeyLenPairs {
		if max < pair[1].(int) {
			max = pair[1].(int)
			gid = pair[0].(int)
		}
	}
	return gid
}

func (sm *ShardCtrler) getMinGID(groupShards map[int][]int) int {
	min := 1<<31 - 1
	gid := 0
	KeyLenPairs := make([][2]interface{}, 0)

	for key, value := range groupShards {
	  KeyLenPairs = append(KeyLenPairs, [2]interface{}{key, len(value)})
	}
  
	sort.Slice(KeyLenPairs, func(i, j int) bool {
		return KeyLenPairs[i][0].(int) < KeyLenPairs[j][0].(int)
	})
	  for _, pair  := range KeyLenPairs {
		  if min > pair[1].(int) {
			  min = pair[1].(int)
			  gid = pair[0].(int)
		  }
	  }
	return gid
}
func (sc *ShardCtrler) addGID(gid int){
	cfg  := &sc.configs[sc.configIdx]
	groupedByGID := sc.groupByGID()

	//if gid is first
	if len(cfg.Groups) == 1{
		for i := 0;i < NShards;i++{
			cfg.Shards[i] = gid
		}
	} else {
		iters := NShards / len(cfg.Groups)

		for i := 0;i < iters;i ++{
			GIDTotake := sc.getMaxGID(groupedByGID)
			cfg.Shards[groupedByGID[GIDTotake][0]] = gid
			groupedByGID[GIDTotake] = groupedByGID[GIDTotake][1:]
		}
	}
}

func (sc *ShardCtrler) delGID(gid int){	cfg  := &sc.configs[sc.configIdx]
	groupedByGID := sc.groupByGID()

	if len(cfg.Groups) == 0{
		cfg.Shards = [NShards]int{}
	} else {
		iters := groupedByGID[gid]
		delete(groupedByGID, gid)
		for _, shard := range iters{
			GIDToGive := sc.getMinGID(groupedByGID)
			cfg.Shards[shard] = GIDToGive
			groupedByGID[GIDToGive] = append(groupedByGID[GIDToGive], shard)
			for key := range groupedByGID {
				sort.Ints(groupedByGID[key])
			}
		}
	}
}

func (sc *ShardCtrler) applyTicker() {
	for {
		select {
			case <-sc.shutdown:
				return
			case applyMsg := <-sc.applyCh:
			op := applyMsg.Command.(Op)
			sc.mu.Lock()
			lastSerial, exists := sc.lastClientSerial[op.ClientId]
			if (!exists || lastSerial < op.Serial) && op.Serial >= 0{
				if op.OpType == "Query"{
					panic("asfd")
				} else if op.OpType == "Join"{
					sc.makeNewConfig()
					KeyLenPairs := make([][2]interface{}, 0)

					for key, value := range op.Servers {
					  KeyLenPairs = append(KeyLenPairs, [2]interface{}{key, value})
					}
					sort.Slice(KeyLenPairs, func(i, j int) bool {
						return KeyLenPairs[i][0].(int) < KeyLenPairs[j][0].(int)
					})
					  for _, pair  := range KeyLenPairs {
						key := pair[0].(int)
						value := pair[1].([]string)
						sc.configs[sc.configIdx].Groups[key] = value
						sc.addGID(key)

					  }

					//fmt.Printf("join: %v shards:%v\n", op.Servers, sc.configs[sc.configIdx].Shards)
				} else if op.OpType == "Leave"{
					sc.makeNewConfig()
					for _, key := range(op.GIDs){
						delete(sc.configs[sc.configIdx].Groups, key)
						sc.delGID(key)
					}
					//fmt.Printf("leave: %v shards:%v\n", op.GIDs, sc.configs[sc.configIdx].Shards)


				} else if op.OpType == "Move"{
					sc.makeNewConfig()
					sc.configs[sc.configIdx].Shards[op.Shard] = op.GID
					//fmt.Printf("move: shard%v to %v shards:%v\n",op.Shard, op.GID, sc.configs[sc.configIdx].Shards)

				}
				sc.lastClientSerial[op.ClientId] = op.Serial
			}
			if _, ok := sc.commandChannel[applyMsg.CommandIndex]; !ok {
				sc.commandChannel[applyMsg.CommandIndex] = make(chan Op, 1)
			}
			ch := sc.commandChannel[applyMsg.CommandIndex]
			sc.mu.Unlock()

			select {
				case <-ch:
				default:
			}
			ch <- op
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant ShardCtrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configIdx = 0

	sc.lastClientSerial = make(map[int]int)
	sc.commandChannel  = make(map[int]chan Op)
	sc.shutdown = make(chan struct{})

	go sc.applyTicker()
	return sc
}


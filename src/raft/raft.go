package raft

//VIP NOTES:
//first index in log is 1

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

type LogEntry struct{
	Command interface{}
	Term int
}

type ServerState int

const (
	Follower    ServerState = iota + 1 
	Candidate                       
	Leader                      
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers:
	currentTerm int 
	votedFor int
	log []LogEntry

	//**** NOTE first index in log is 1

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	nextIndex []int
	matchIndex []int


	//applyCh
	applyCh chan ApplyMsg

	//other election stuff
	serverState ServerState
	lastHeartBeat time.Time

	//for candidate
	votesCount int


}

func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{CommandValid: true,
							Command: rf.log[i].Command,
							CommandIndex: i,
		}

		rf.lastApplied++
		//DPrintf("server:%v last applied:%v\n", rf.me, rf.lastApplied)

	}
}

//lock must be held before calling this
func (rf *Raft) isCandidateAtLeastUpToDate(LastLogIndex int, LastLogTerm int)bool{

	//DPrintf("current term:%v index:%v given term%v index%v\n", rf.getLastLogTerm(), rf.getLastLogIndex(), LastLogTerm, LastLogIndex)
	if(LastLogTerm == rf.getLastLogTerm() ){
		return LastLogIndex >= rf.getLastLogIndex()
	}
	return LastLogTerm > rf.getLastLogTerm()

}

//lock must be held before calling this
func (rf *Raft) getLastLogIndex()int {
	return len(rf.log) - 1
}

//lock must be held before calling this
func (rf *Raft) getLastLogTerm()int {
	return rf.log[rf.getLastLogIndex()].Term
}

//lock must be held before calling this
func (rf *Raft) ToFollower(Term int){
	// if rf.serverState == Leader{
	// 	DPrintf("Leader %v stepped down from preTerm%v to newTerm%v\n", rf.me, rf.currentTerm, Term)
	// }
	rf.currentTerm = Term
	rf.votedFor = -1
	rf.serverState = Follower
}
//lock must be held before calling this

func (rf *Raft) ToCandidate(){
	rf.serverState = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesCount = 1
}
//lock must be held before calling this
func (rf *Raft) ToLeader(){
	rf.serverState = Leader
	
	//DPrintf("server:%v Term:%v is leader\n ", rf.me, rf.currentTerm)

	//reinitialize nextIndex, matchIndex

	//When a leader first comes to power,
	// it initializes all nextIndex values to the index just after the
	// last one in its log
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i:= 0; i < len(rf.nextIndex); i++{
		rf.nextIndex[i] = rf.getLastLogIndex()+1
		rf.matchIndex[i] = 0
	}
	//DPrintf("server:%v Term:%v is leader\n ", rf.me, rf.currentTerm)


}
//lock must be held before calling this
func (rf *Raft) VotingFor(CandidateId int){
	rf.votedFor = CandidateId
	rf.lastHeartBeat = time.Now()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("current server%v status%v\n", rf.me, rf.serverState)
	
	return rf.currentTerm, rf.serverState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm) 
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log) 
	rf.persister.Save(buffer.Bytes(), nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// bootstrap without any state?
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (2C).
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	decoder.Decode(&rf.currentTerm) 
	decoder.Decode(&rf.votedFor) 
	decoder.Decode(&rf.log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidatId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm{
		rf.ToFollower(args.Term)
	}

	if args.Term < rf.currentTerm{
		//1. Reply false if term < currentTerm (§5.1)
		reply.VoteGranted = false
	} else if( (rf.votedFor == -1 || rf.votedFor == args.CandidatId) && rf.isCandidateAtLeastUpToDate(args.LastLogIndex, args.LastLogTerm)){
		//2. If votedFor is null or candidateId, and candidate’s log is at
		//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		reply.VoteGranted = true
		rf.VotingFor(args.CandidatId)
		//DPrintf("server: %v term:%v Voted: %v\n", rf.me, rf.currentTerm, args.CandidatId)
	} else {
		reply.VoteGranted = false
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in -structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok{
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term > rf.currentTerm{
		rf.ToFollower(args.Term)
	}

	if rf.serverState != Candidate {
		return
	}


	if reply.VoteGranted{
		rf.votesCount += 1
		//DPrintf("server:%v term: %v votes:%v lenpeers%v\n", rf.me, rf.currentTerm, rf.votesCount, )
		if rf.votesCount == len(rf.peers)/2 + 1{
			rf.ToLeader()
		}
	}
	
}



// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LogEntries []LogEntry
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term int
	Success bool
	XTerm  int //term in the conflicting entry (if any)
    XIndex int //index of first entry with that term (if any)
    XLen   int// log length
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm{
		rf.ToFollower(args.Term)
	}

	if args.Term < rf.currentTerm {
 		//DPrintf("failed 1, leader %v term=%v send to follower %v term=%v\n", args.LeaderId,args.Term, rf.me, rf.currentTerm)

		reply.Success = false
		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = len(rf.log)

		//1. Reply false if term < currentTerm (§5.1)
	} else if(rf.getLastLogIndex() < args.PrevLogIndex){
		//2. Reply false if log doesn’t contain an entry at prevLogIndex
		//whose term matches prevLogTerm (§5.3)

		//follower log shorter than leader's
 		//DPrintf("failed 2: Leader %v send to follower %v rf.lastindex=%v, args.previndex=%v\n",args.LeaderId, rf.me, rf.getLastLogIndex(), args.PrevLogIndex)
		reply.Success = false
		rf.lastHeartBeat = time.Now()
		reply.XIndex = rf.getLastLogIndex()
		reply.XLen = len(rf.log)
		reply.XTerm = -1

	}else if(rf.log[args.PrevLogIndex].Term != args.PrevLogTerm){
		//2. Reply false if log doesn’t contain an entry at prevLogIndex
	   //whose term matches prevLogTerm (§5.3)


		//conflict 
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XLen = len(rf.log)
		for i := args.PrevLogIndex; i >= 0 && rf.log[i].Term == rf.log[args.PrevLogIndex].Term; i-- {
			reply.XIndex = i
		}

		//DPrintf("failed 2: Leader %v send to follower %v rf.lastindex=%v, args.previndex=%v\n",args.LeaderId, rf.me, rf.getLastLogIndex(), args.PrevLogIndex)
	   reply.Success = false
	   rf.lastHeartBeat = time.Now()


   } else {

		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		reply.Success = true
		rf.lastHeartBeat = time.Now()

		//4. Append any new entries not already in the log
		i := args.PrevLogIndex + 1
		j := 0
		for i < len(rf.log) && j < len(args.LogEntries){
			if rf.log[i].Term != args.LogEntries[j].Term{
				//3. If an existing entry conflicts with a new one (same index
				// but different terms), delete the existing entry and all that
				// follow it (§5.3)
				break
			}
			i++
			j++
		}
		rf.log = append(rf.log[:i], (args.LogEntries[j:])...)
// 		if j < len(args.LogEntries) {
// 			DPrintf("Leader %v term=%v overwriting logs at follower %v term=%v: len(rf.log)=%v i=%v len(args.entries)=%v j=%v\n", args.LeaderId, args.Term, rf.me, rf.currentTerm, len(rf.log), i, len(args.LogEntries),j)
// 		}

		//5. If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex{
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			//DPrintf("follower %v commit index:%v\n", rf.me, rf.commitIndex)
			go rf.commitLogs()
		}
	}

}

// example code to send a AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendAppendEntries(peer int){

	rf.mu.Lock()
	if rf.serverState != Leader{
		rf.mu.Unlock()
		return 
	}
	args := AppendEntriesArgs{Term: rf.currentTerm, 
		LeaderId: rf.me, 
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm: rf.log[rf.nextIndex[peer] - 1].Term,
		LogEntries: rf.log[rf.nextIndex[peer]:],
		LeaderCommit: rf.commitIndex}
	
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.ToFollower(reply.Term)
	}
	if reply.Term < rf.currentTerm || rf.serverState != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Success{
		rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PrevLogIndex + len(args.LogEntries))
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

	} else{
		if reply.XTerm >= 0{ //follower's log is at least as long as leader's log
			leaderLastIndexForXTerm := rf.getLastLogIndex()
			for ;leaderLastIndexForXTerm >= 0 && rf.log[leaderLastIndexForXTerm].Term != reply.XTerm; leaderLastIndexForXTerm--{

			}
			if leaderLastIndexForXTerm == -1{//Case 1: leader doesn't have XTerm:
				rf.nextIndex[peer] = reply.XIndex
			} else {//Case 2: leader has XTerm:
				rf.nextIndex[peer] = leaderLastIndexForXTerm
			}
		} else {  //Case 3: follower's log is too short:
			rf.nextIndex[peer] = reply.XLen
		}
		rf.matchIndex[peer] = rf.nextIndex[peer]  - 1

	}

	for n := rf.getLastLogIndex(); n >= rf.commitIndex; n-- {
		cnt := 1
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		for i := range(rf.peers){
			if i != rf.me && rf.matchIndex[i] >= n {
				cnt++
			}
		}
		if cnt > len(rf.peers) / 2 {
			rf.commitIndex = n
			go rf.commitLogs()
			break;
		}
	}
	//DPrintf("server%v commit index: %v\n", rf.me, rf.commitIndex)					
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer 	rf.persist()

	if(rf.serverState != Leader){
		return -1, rf.currentTerm, false
	}
	//DPrintf("command at leader: %v\n", rf.me)
	// If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})

	return rf.getLastLogIndex(), rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ElectionTicker() {

	for !rf.killed(){
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		lastHeartBeat := rf.lastHeartBeat
		serverState := rf.serverState

		if serverState == Leader || time.Since(lastHeartBeat) < getRandtime(100,150){
			rf.mu.Unlock()
			time.Sleep(getRandtime(150, 250))
			continue
		}
		//DPrintf("server %v starts election\n", rf.me)
		//start elections
		rf.ToCandidate()
		rf.persist()

		//reset the election timeout 
		rf.lastHeartBeat = time.Now()

		requestVoteArgs := RequestVoteArgs{
			Term: rf.currentTerm,
			 CandidatId: rf.me,
			LastLogIndex: rf.getLastLogIndex(),
			LastLogTerm: rf.getLastLogTerm(),
		}
		rf.mu.Unlock()
		for peer := 0; peer < len(rf.peers); peer++{
			if peer == rf.me{
				continue
			}
			go rf.sendRequestVote(peer, &requestVoteArgs, &RequestVoteReply{})
		}
		
		// pause for a random amount of time between 150 and 250
		// milliseconds.
		time.Sleep(getRandtime(100, 150))
	}
}

//leader operatinos goroutines:
//If last log index ≥ nextIndex for a follower: send
// AppendEntries RPC with log entries starting at nextIndex
// • If successful: update nextIndex and matchIndex for
// follower (§5.3)
// • If AppendEntries fails because of log inconsistency:
// decrement nextIndex and retry (§5.3)
// • If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) LeaderAppendEntriesTicker() {

	for !rf.killed(){

		rf.mu.Lock()
		if rf.serverState != Leader{
			rf.mu.Unlock()
			time.Sleep(getRandtime(100, 100))
			continue
		}
		//DPrintf("go append entries from server:%v commit%v lastapplied%v\n\n", rf.me, rf.commitIndex, rf.lastApplied)

		//DPrintf("leader %v is sending %v\n ", rf.me, time.Now())
		//send heartbeats
		//reset the election timeout 
		rf.lastHeartBeat = time.Now()
		

		rf.mu.Unlock()
		for peer := 0; peer < len(rf.peers); peer++{
			if peer == rf.me{
				continue
			}
			go rf.sendAppendEntries(peer);
		}
		
		// pause for a random amount of time between 100 and 150
		// milliseconds.
		time.Sleep(getRandtime(100, 100))
	}
}


// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (3A, 3B, 3C).		

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Term:0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	//only iniitlize at ToLeader func
	// rf.nextIndex = make([]int, len(rf.peers))
	// rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh

	rf.lastHeartBeat = time.Now()
	rf.serverState = Follower

	rf.votesCount = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ElectionTicker()
	go rf.LeaderAppendEntriesTicker()
	
	return rf
}

/*
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

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	//3D
	lastIncludedIndex int
	lastIncludedTerm int
	snapshot []byte
	applyChProxy chan ApplyMsg
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
func (rf *Raft) logLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}

//lock must be held before calling this
func (rf *Raft) getLogEntry(i int) LogEntry{
	return rf.log[i - rf.lastIncludedIndex]
}
//lock must be held before calling this
//slice[l,r) including l but excluding r
//indexes are absolute(with addition of base = rf.lastIncludedIndex + 1)
func (rf *Raft) getLogSlice(l int, r int) []LogEntry{
	base := rf.lastIncludedIndex
	l -= base
	r -= base
	//DPrintf("getLogSlice: base:%v l:%v r:%v len:%v", base,l ,r,rf.getLastLogIndex()+1)
	return append(make([]LogEntry, 0),rf.log[l:r]...)
}

//lock must be held before calling this
func (rf *Raft) getLastLogIndex()int {
	return rf.logLen()- 1
}

//lock must be held before calling this
func (rf *Raft) getLastLogTerm()int {
	return rf.getLogEntry(rf.getLastLogIndex()).Term
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
	DPrintf("tocandidate %v", rf.me)
}
//lock must be held before calling this
func (rf *Raft) ToLeader(){
	if rf.serverState != Candidate{
		return
	}
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

func (rf *Raft) commitLogs() {
        for {
            entry := <-rf.applyChProxy
            // Apply entry here
			rf.applyCh <- entry
            //fmt.Println("Applying entry:", entry)

        }

	// for !rf.killed(){

	// 	rf.mu.Lock()
	// 	if !(rf.lastApplied < rf.commitIndex){
	// 		rf.mu.Unlock()
	// 		time.Sleep(getRandtime(400, 500))
	// 		continue
	// 	}

	// 	msg := ApplyMsg{CommandValid: true,
	// 		Command: rf.getLogEntry(rf.lastApplied+1).Command,
	// 		CommandIndex: rf.lastApplied + 1,
	// 		}
	// 		rf.lastApplied++
	// 		rf.mu.Unlock()
	// 		rf.applyCh <- msg
	// 		DPrintf("server %v, last applied%v", rf.me, rf.lastApplied)
	// }
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

	rf.snapshot = nil
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.applyChProxy = make(chan ApplyMsg, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ElectionTicker()
	go rf.LeaderAppendEntriesTicker()
	go rf.commitLogs()
	return rf
}
*/

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

	"math/rand"
	"sort"
	"sync"
	"time"

	//	"6.5840/labgob"

	"6.5840/labrpc"
)


type LogEntry struct {
	Term    int
	Command interface{}
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


	//elections 
	serverState ServerState
	lastHeartBeat time.Time

	//applyCh
	applyCh chan ApplyMsg
	applyChProxy chan ApplyMsg

	//for candidate
	votesCount int

	//snapshot
	lastIncludedIndex int
	lastIncludedTerm int

/////
	//channel
	killCh  chan bool     //for Kill()

	//handle rpc
	voteCh   chan bool
	appendCh chan bool
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
func (rf *Raft) logLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}

//lock must be held before calling this
func (rf *Raft) getLogEntry(i int) LogEntry{
	return rf.log[i - rf.lastIncludedIndex]
}
//lock must be held before calling this
//slice[l,r) including l but excluding r
//indexes are absolute(with addition of base = rf.lastIncludedIndex + 1)
func (rf *Raft) getLogSlice(l int, r int) []LogEntry{
	base := rf.lastIncludedIndex
	l -= base
	r -= base
	//DPrintf("getLogSlice: base:%v l:%v r:%v len:%v", base,l ,r,rf.getLastLogIndex()+1)
	return append(make([]LogEntry, 0),rf.log[l:r]...)
}

//lock must be held before calling this
func (rf *Raft) getLastLogIndex()int {
	return rf.logLen()- 1
}

//lock must be held before calling this
func (rf *Raft) getLastLogTerm()int {
	return rf.getLogEntry(rf.getLastLogIndex()).Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("current server%v status%v\n", rf.me, rf.serverState)

	return rf.currentTerm, rf.serverState == Leader
}


/////////////

func send(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func (rf *Raft) updateMatchIndex(server int, matchIdx int) {
	rf.matchIndex[server] = matchIdx
	rf.nextIndex[server] = matchIdx + 1
	rf.updateCommitIndex()
}

func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.logLen() - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.getLogEntry(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.Commit()
	}
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

	if(rf.serverState != Leader){
		return -1, rf.currentTerm, false
	}
	//DPrintf("command at leader: %v\n", rf.me)
	// If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	////////
	rf.persist(rf.persister.ReadSnapshot())
	rf.broadcastHeartbeat()

	return rf.getLastLogIndex(), rf.currentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	send(rf.killCh)
}
func (rf *Raft) commitLogs() {
	for {
		entry := <-rf.applyChProxy
		// Apply entry here
		rf.applyCh <- entry
		//fmt.Println("Applying entry:", entry)

	}
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.serverState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.applyChProxy = make(chan ApplyMsg,100)
	rf.voteCh = make(chan bool, 1)
	rf.appendCh = make(chan bool, 1)
	rf.killCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.service()
	go rf.commitLogs()
	return rf
}

func (rf *Raft) service() {
	for {
		select {
		case <-rf.killCh:
			return
		default:
		}

		rf.mu.Lock()
		state := rf.serverState
		rf.mu.Unlock()

		electionTime := time.Duration(rand.Intn(200)+300) * time.Millisecond
		heartbeatTime := time.Duration(100) * time.Millisecond
		switch state {
		case Follower, Candidate:
			select {
			case <-rf.voteCh:
			case <-rf.appendCh:
			case <-time.After(electionTime):
				rf.mu.Lock()
				rf.ToCandidate()
				rf.mu.Unlock()
			}
		case Leader:
			time.Sleep(heartbeatTime)
			rf.broadcastHeartbeat()
		}
	}
}
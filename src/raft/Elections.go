/*

func (rf *Raft) ElectionTicker() {

	for !rf.killed(){
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		lastHeartBeat := rf.lastHeartBeat
		serverState := rf.serverState

		if serverState == Leader || time.Since(lastHeartBeat) < getRandtime(800,1200){
			rf.mu.Unlock()
			time.Sleep(getRandtime(400, 500))
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
		time.Sleep(getRandtime(400, 500))
	}
}

*/

package raft

import (
	"time"
)

//lock must be held before calling this
func (rf *Raft) isCandidateAtLeastUpToDate(LastLogIndex int, LastLogTerm int)bool{

	//DPrintf("current term:%v index:%v given term%v index%v\n", rf.getLastLogTerm(), rf.getLastLogIndex(), LastLogTerm, LastLogIndex)
	if(LastLogTerm == rf.getLastLogTerm() ){
		return LastLogIndex >= rf.getLastLogIndex()
	}
	return LastLogTerm > rf.getLastLogTerm()

}
//lock must be held before calling this
func (rf *Raft) ToFollower(Term int){
	// if rf.serverState == Leader{
	// 	DPrintf("Leader %v stepped down from preTerm%v to newTerm%v\n", rf.me, rf.currentTerm, Term)
	// }
	rf.currentTerm = Term
	rf.votedFor = -1
	rf.serverState = Follower
	rf.persist(rf.persister.ReadSnapshot())

}
//lock must be held before calling this

func (rf *Raft) ToCandidate(){
	rf.serverState = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesCount = 1
	DPrintf("tocandidate %v", rf.me)
	rf.persist(rf.persister.ReadSnapshot())
	go rf.broadcastVoteReq()
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


type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.ToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		//1. Reply false if term < currentTerm (§5.1)
		reply.VoteGranted = false
	} else if( (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateAtLeastUpToDate(args.LastLogIndex, args.LastLogTerm)){
		//2. If votedFor is null or candidateId, and candidate’s log is at
		//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		//rf.serverState = Follower
		rf.persist(rf.persister.ReadSnapshot())

		rf.lastHeartBeat = time.Now()
		send(rf.voteCh)
		//DPrintf("server: %v term:%v Voted: %v\n", rf.me, rf.currentTerm, args.CandidatId)
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendRequestVote(server int){
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm: rf.getLastLogTerm(),
	}
	rf.mu.Unlock()
	reply := &RequestVoteReply{}	

	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok{
		return 
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.ToFollower(reply.Term)
	}
	if rf.serverState != Candidate || rf.currentTerm != args.Term {
		return
	}

	if reply.VoteGranted {
		rf.votesCount += 1
		if rf.votesCount == len(rf.peers)/2 + 1 {
			rf.ToLeader()
			//rf.lastHeartBeat = time.Now()
			//send(rf.voteCh)
			rf.broadcastHeartbeat()
		}
	}

}

func (rf *Raft) broadcastVoteReq() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		 go rf.sendRequestVote(i);
		
	}
}

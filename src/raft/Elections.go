package raft

import "time"

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
	//DPrintf("tocandidate %v", rf.me)
	rf.persist(rf.persister.ReadSnapshot())
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
	defer 		rf.persist(rf.persister.ReadSnapshot())


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
	defer 		rf.persist(rf.persister.ReadSnapshot())


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

func (rf *Raft) ElectionTicker() {

	for !rf.killed(){
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		lastHeartBeat := rf.lastHeartBeat
		serverState := rf.serverState

		if serverState == Leader || time.Since(lastHeartBeat) < getRandtime(300,500){
			rf.mu.Unlock()
			time.Sleep(getRandtime(100, 200))
			continue
		}
		//DPrintf("server %v starts election\n", rf.me)
		//start elections
		rf.ToCandidate()
		rf.persist(rf.persister.ReadSnapshot())

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
		time.Sleep(getRandtime(100, 200))
	}
}

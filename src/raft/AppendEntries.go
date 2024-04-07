package raft

import "time"

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LogEntries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (3A).
	Term int
	Success bool
	XTerm  int //term in the conflicting entry (if any)
    XIndex int //index of first entry with that term (if any)
    XLen   int// log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
		// Your code here (3A, 3B).
		rf.mu.Lock()
		defer rf.mu.Unlock()
	
		DPrintf("append from leader%v to server%v, with base-1=%v lastprevlog:%v, lastprevTerm:%v logentries:%v\n",
		args.LeaderId, rf.me, rf.lastIncludedIndex, args.PrevLogIndex, args.PrevLogTerm, len(args.LogEntries))
	
		if args.Term > rf.currentTerm{
			rf.ToFollower(args.Term)
		}
	
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = 0
		reply.XLen = rf.logLen()
		rf.lastHeartBeat = time.Now()
		send(rf.electionResetCh)

		if args.Term < rf.currentTerm {
			//1. Reply false if term < currentTerm (§5.1)
	
			 //DPrintf("failed 1, leader %v term=%v send to follower %v term=%v\n", args.LeaderId,args.Term, rf.me, rf.currentTerm)
			return
	
		} else if(!(args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogIndex < rf.logLen())){
			//2. Reply false if log doesn’t contain an entry at prevLogIndex
	
			//follower log shorter than leader's
			 //DPrintf("failed 2: Leader %v send to follower %v rf.lastindex=%v, args.previndex=%v\n",args.LeaderId, rf.me, rf.getLastLogIndex(), args.PrevLogIndex)
			reply.XIndex = rf.getLastLogIndex()
			return
		}else if(rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm) {
			//2. Reply false if log contains an entry at prevLogIndex but term mismatch
	
			//conflict
			reply.XTerm = rf.getLogEntry(args.PrevLogIndex).Term
			for i := rf.lastIncludedIndex; i < rf.logLen(); i++ {
				if rf.getLogEntry(i).Term == reply.XTerm {
					reply.XIndex = i
					break
				}
			}
			//DPrintf("failed 2: Leader %v send to follower %v rf.lastindex=%v, args.previndex=%v\n",args.LeaderId, rf.me, rf.getLastLogIndex(), args.PrevLogIndex)
	
	   } else {
			reply.Success = true
	
			//4. Append any new entries not already in the log
			//rf.log = append( rf.getLogSlice(rf.lastIncludedIndex, args.PrevLogIndex+1), (args.LogEntries)...)
	// 		if j < len(args.LogEntries) {
	// 			DPrintf("Leader %v term=%v overwriting logs at follower %v term=%v: len(rf.log)=%v i=%v len(args.entries)=%v j=%v\n", args.LeaderId, args.Term, rf.me, rf.currentTerm, len(rf.log), i, len(args.LogEntries),j)
	// 		}

			l := args.PrevLogIndex + 1
			r := 0
			for ;l < rf.logLen() && r < len(args.LogEntries) && rf.getLogEntry(l).Term == args.LogEntries[r].Term;{
				l++
				r++
			}
			if r < len(args.LogEntries){//VIP if args.logentries finishes before l gets to loglen
				rf.log = append(rf.getLogSlice(rf.lastIncludedIndex, l), args.LogEntries[r:]...)
				rf.persist(rf.persister.ReadSnapshot())
			}

			//5. If leaderCommit > commitIndex, set commitIndex =
			//min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex{
				rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
				DPrintf("follower %v commit index:%v\n", rf.me, rf.commitIndex)
				rf.Commit()
			}
		}
}
//called while aquiring the lock
func (rf *Raft) sendAppendEntries(peer int) {
	
		args := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			rf.nextIndex[peer]-1,
			rf.getLogEntry(rf.nextIndex[peer]-1).Term,
			rf.getLogSlice(rf.nextIndex[peer], rf.getLastLogIndex()+1),
			rf.commitIndex,

		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok || rf.serverState != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.ToFollower(reply.Term)
			return
		}

		if !reply.Success {
			if reply.XTerm == -1 {
				//Case 3: follower's log is too short:
    			//nextIndex = XLen
				rf.nextIndex[peer] = min(rf.logLen(), reply.XLen)
			} else {
				leaderLastXTerm := -1
				i := rf.lastIncludedIndex;
				for ; i < rf.logLen() && rf.getLogEntry(i).Term != reply.XTerm; i++ {
				}
				for ; i < rf.logLen() && rf.getLogEntry(i).Term == reply.XTerm ; i++{
					leaderLastXTerm = i
				}
				if leaderLastXTerm == -1{
					//  Case 1: leader doesn't have XTerm:	
					//  nextIndex = XIndex
					rf.nextIndex[peer] = reply.XIndex
				} else{
					//  Case 2: leader has XTerm:
    				// nextIndex = leader's last entry for XTerm
					rf.nextIndex[peer] = leaderLastXTerm
				}
			}
		} else {
			rf.updatePeerMatch(peer, args.PrevLogIndex+len(args.LogEntries))
			rf.updateCommitIndex()
		}
}

func (rf *Raft) HeartBeatTicker() {
	DPrintf("%d send logs: %v", rf.me, rf.log)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(peer int) {
				rf.mu.Lock()
				if rf.serverState != Leader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[peer] <= rf.lastIncludedIndex {
					rf.sendInstallSnapshot(peer)
				}else {
					rf.sendAppendEntries(peer)
				}
		}(i)
	}
}


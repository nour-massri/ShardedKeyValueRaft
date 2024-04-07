/*
	update commit index

	//for leaders rf.commitIndex >= rf.logStartIndex:
	// for n := rf.getLastLogIndex(); n > rf.commitIndex; n-- {
	// 	cnt := 1
	// 	if rf.getLogEntry(n).Term != rf.currentTerm {
	// 		continue
	// 	}
	// 	for i := range(rf.peers){
	// 		if i != rf.me && rf.matchIndex[i] >= n {
	// 			cnt++
	// 		}
	// 	}
	// 	if cnt > len(rf.peers) / 2 {
	// 		rf.commitIndex = n
	// 		DPrintf("leader %v commit index:%v", rf.me, rf.commitIndex)
	// 		//go rf.commitLogs()
	// 		break;
	// 	}
	// }
	//DPrintf("server%v commit index: %v\n", rf.me, rf.commitIndex)
}
}*/

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
			// rf.log = append(rf.getLogSlice(rf.lastIncludedIndex, args.PrevLogIndex + 1), args.LogEntries...)
			// rf.persist(rf.persister.ReadSnapshot())

			index := args.PrevLogIndex
			for i := 0; i < len(args.LogEntries); i++ {
				index++
				if index < rf.logLen() {
					if rf.getLogEntry(index).Term == args.LogEntries[i].Term {
						continue
					} else {
						rf.log = rf.log[:index-rf.lastIncludedIndex]
					}
				}
				// append any new entries not already in the logs
				rf.log = append(rf.log, args.LogEntries[i:]...)
				rf.persist(rf.persister.ReadSnapshot())
				break
			}

			//5. If leaderCommit > commitIndex, set commitIndex =
			//min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex{
				rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
				DPrintf("follower %v commit index:%v\n", rf.me, rf.commitIndex)
				//go rf.commitLogs()
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
		if !ok || rf.serverState != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currentTerm {
			rf.ToFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		// update nextIndex and matchIndex for follower
		if reply.Success {
			rf.matchIndex[peer] = args.PrevLogIndex+len(args.LogEntries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.updateCommitIndex()
			rf.mu.Unlock()
			return
		} else {
			// decrement nextIndex and retry
			index := reply.XIndex
			if reply.XTerm != -1 {
				logSize := rf.logLen()
				for i := rf.lastIncludedIndex; i < logSize; i++ {
					if rf.getLogEntry(i).Term != reply.XTerm {
						continue
					}
					for i < logSize && rf.getLogEntry(i).Term == reply.XTerm {
						i++
					}
					index = i
				}
			}
			rf.nextIndex[peer] = min(rf.logLen(), index)
			rf.mu.Unlock()
		}
	
}

func (rf *Raft) broadcastHeartbeat() {
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

				if rf.nextIndex[peer]-rf.lastIncludedIndex < 1 {
					rf.sendInstallSnapshot(peer)
					return
				}
				rf.sendAppendEntries(peer)
		}(i)
	}
}

// Commit should be called after commitIndex updated
func (rf *Raft) Commit() {
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		log := rf.getLogEntry(rf.lastApplied)
		applyMsg := ApplyMsg{CommandValid: true, Command: log.Command, CommandIndex: rf.lastApplied}
		rf.applyChProxy <- applyMsg
	}
}
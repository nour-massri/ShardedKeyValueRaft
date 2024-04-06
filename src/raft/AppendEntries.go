/*
package raft

import (
	"sort"
	"time"
)


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
	//defer rf.persist()

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
			rf.persist()
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

func (rf *Raft) sendAppendEntries(peer int, args AppendEntriesArgs, reply AppendEntriesReply){

	ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.ToFollower(reply.Term)
		return
	}
	if rf.serverState != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		rf.updateMatchIndex(peer, args.PrevLogIndex+len(args.LogEntries))
		return
	} else {
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
	}
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
			time.Sleep(getRandtime(300, 400))
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
			go func(peer int){
				//DPrintf("leader %v is sending  to server %v\n ", rf.me, peer)
				//3D: either send append entries or install snapshot dependong in rf.logStartIndex <= rf.nextIndex[peer]
				rf.mu.Lock()
				if rf.serverState != Leader{
					rf.mu.Unlock()
					return
				}
				nextIndexPeer := rf.nextIndex[peer]
				lastIncludedIndex := rf.lastIncludedIndex

				if nextIndexPeer > lastIncludedIndex {
					args := AppendEntriesArgs{Term: rf.currentTerm,
						LeaderId: rf.me,
						PrevLogIndex: rf.nextIndex[peer] - 1,
						PrevLogTerm: rf.getLogEntry(rf.nextIndex[peer] - 1).Term,
						LogEntries: rf.getLogSlice(rf.nextIndex[peer], rf.getLastLogIndex()+1),
						LeaderCommit: rf.commitIndex}
					rf.mu.Unlock()
					rf.sendAppendEntries(peer, args, AppendEntriesReply{});
					//DPrintf("leader %v sent  to server %v\n ", rf.me, peer)

				} else{
					args := InstallSnapshotsArgs{Term: rf.currentTerm,
						LeaderId: rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm: rf.lastIncludedTerm,
						Data: rf.snapshot,
					}
					rf.mu.Unlock()

					rf.sendInstallSnapshot(peer, args, InstallSnapshotsReply{})
				}
			}(peer)
		}

		//DPrintf("leader %v is done \n ", rf.me)

		// pause for a random amount of time between 100 and 150
		// milliseconds.
		time.Sleep(getRandtime(300, 400))
	}
}

// Commit should be called after commitIndex updated
func (rf *Raft) Commit() {

	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		log := rf.getLogEntry(rf.lastApplied)
		applyMsg := ApplyMsg{CommandValid: true,  Command:  log.Command, CommandIndex: rf.lastApplied}
		rf.applyChProxy <- applyMsg
	}
}*/

package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.ToFollower(args.Term)
	}
	send(rf.appendCh)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	// reply false if logs doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	prevLogIndexTerm := -1
	logSize := rf.logLen()
	if args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogIndex < rf.logLen() {
		prevLogIndexTerm = rf.getLogEntry(args.PrevLogIndex).Term
	}

	// check terms
	if prevLogIndexTerm != args.PrevLogTerm {
		reply.ConflictIndex = logSize
		if prevLogIndexTerm == -1 {
			// return with conflictIndex = len(logs) and conflictTerm = -1
		} else {
			reply.ConflictTerm = prevLogIndexTerm
			for i := rf.lastIncludedIndex; i < logSize; i++ {
				if rf.getLogEntry(i).Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
		}
		return
	}

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	// if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all follow it (§5.3)
	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index < logSize {
			if rf.getLogEntry(index).Term == args.Entries[i].Term {
				continue
			} else {
				rf.log = rf.log[:index-rf.lastIncludedIndex]
			}
		}
		// append any new entries not already in the logs
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist(rf.persister.ReadSnapshot())
		break
	}

	// if LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of the last logs entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.Commit()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(peer int) {
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		rf.nextIndex[peer]-1,
		rf.getLogEntry(rf.nextIndex[peer]-1).Term,
		rf.commitIndex,
		append(make([]LogEntry, 0), rf.log[rf.nextIndex[peer]-rf.lastIncludedIndex:]...),
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
		rf.updateMatchIndex(peer, args.PrevLogIndex+len(args.Entries))
		rf.mu.Unlock()
		return
	} else {
		// decrement nextIndex and retry
		index := reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			logSize := rf.logLen()
			for i := rf.lastIncludedIndex; i < logSize; i++ {
				if rf.getLogEntry(i).Term != reply.ConflictTerm {
					continue
				}
				for i < logSize && rf.getLogEntry(i).Term == reply.ConflictTerm {
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
			for {
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
				
			}
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
		//rf.applyCh <- applyMsg
		rf.applyChProxy <- applyMsg

	}
}
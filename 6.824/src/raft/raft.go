package raft

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

import "sync"
import "sync/atomic"
import "../labrpc"
import "sort"
import "log"
import "bytes"
import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type state int

const (
	leader state = iota
	follower
	candidate
)

type LogEntries struct {
	Command interface{}
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg
	
	electionTick *ticker
	heartbeatTick *ticker
	
	vote int
	state
	
	// All servers
	currentTerm int  // persistent
	votedFor int     // persistent
	log []LogEntries // persistent
	commitIndex int
	lastApplied int
	
	// Leaders
	nextIndex []int
	matchIndex []int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	return rf.currentTerm, rf.state == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	log.Printf("%d write persist [term: %d, voted: %d, log: %v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntries
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil || 
	   d.Decode(&logs) != nil {
		log.Printf("%d read persist error\n", rf.me)
		// TODO error...
	} else {
		log.Printf("%d read persist [term: %d, voted: %d, log: %v]\n", rf.me, currentTerm, votedFor, logs)
		
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntries
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesReply struct {
	Term int
	Success bool
	
	// fast backup
	XTerm int
	XIndex int
	XLen int
}

// TODO more effective: lock, ...

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		return
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	log.Printf("%d receive RequestVote from %d\n", rf.me, args.CandidateId)
	
	if args.Term > rf.currentTerm {
		log.Printf("%d new term %d -> %d\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state != follower {
			rf.turnFollower()
		}
	}
	
	reply.Term = rf.currentTerm
	
	if args.Term < rf.currentTerm ||
	   rf.votedFor != -1 {
		log.Printf("%d reject vote to %d: term %d candidate term %d votedFor: %d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.votedFor)
		reply.VoteGranted = false
		return
	}
	
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	if args.LastLogTerm < lastLogTerm ||
	   (args.LastLogTerm == lastLogTerm &&
	    args.LastLogIndex < lastLogIndex) {
		log.Printf("%d reject vote to %d: LastLogTerm %d LastLogIndex %d but candidate LastLogTerm %d LastLogIndex %d\n", rf.me, args.CandidateId, lastLogTerm, lastLogIndex, args.LastLogTerm, args.LastLogIndex)
		reply.VoteGranted = false
		return
	}
	
	log.Printf("%d vote to %d\n", rf.me, args.CandidateId)
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	log.Printf("[follower: %d, term: %d] receive AppendEntries with [PrevLogIndex: %d, PrevLogTerm: %d, entries: %v] from [leader: %d, term: %d]\n", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderId, args.Term)
	
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != follower {
			rf.turnFollower()
		}
	}
	
	reply.Term = rf.currentTerm
	
	if args.Term < rf.currentTerm {
		log.Printf("[follower: %d, term: %d] reject AppendEntries from [leader: %d, term: %d]: term not match\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Success = false
		return
	}
	
	rf.electionTick.reset()
	
	if args.PrevLogIndex >= len(rf.log) ||
	   rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		log.Printf("[follower: %d, term: %d] reject AppendEntries from [leader: %d, term: %d]: need PrevLogIndex %d but got PrevLogIndex %d\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, len(rf.log) - 1, args.PrevLogIndex)
		reply.Success = false
		
		// fast backup
		if args.PrevLogIndex >= len(rf.log) {
			reply.XTerm = -1
			reply.XLen = len(rf.log)
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			var xindex int
			// TODO binary search
			for xindex = args.PrevLogIndex;
			    xindex >= 1 && rf.log[xindex].Term == reply.XTerm;
			    xindex-- {}
			reply.XIndex = xindex + 1
		}
		
		return
	}
	
	rf.log = rf.log[:args.PrevLogIndex + 1]
	rf.log = append(rf.log, args.Entries...)
	
	newCommitIndex := min(args.LeaderCommit, len(rf.log) - 1)
	if newCommitIndex > rf.commitIndex {
		log.Printf("[follower: %d, term: %d] commit: [%d, %d]\n", rf.me, rf.currentTerm, rf.commitIndex + 1, newCommitIndex)
		for i := rf.commitIndex + 1;
		    i <= newCommitIndex;
		    i++ {
			log.Printf("[follower: %d, term: %d] apply command [%v] with index [%d]\n", rf.me, rf.currentTerm, rf.log[i].Command, i)
			rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
		}
		rf.commitIndex = newCommitIndex
	}
	
	log.Printf("[follower: %d, term: %d] success AppendEntries with [PrevLogIndex: %d PrevLogTerm: %d] from [leader: %d, term: %d]\n", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderId, args.Term)
	reply.Success = true
	
	rf.persist()
}

//
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
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.receiveVote(server, args, reply)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.hearbeatFeedback(server, args, reply)
	}
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.state != leader {
		return -1, -1, false
	}
	
	log.Printf("[leader: %d, term: %d] append new command [%v] in index [%d]", rf.me, rf.currentTerm, command, len(rf.log))
	
	rf.log = append(rf.log, LogEntries{command, rf.currentTerm})
	rf.persist()
	return len(rf.log) - 1, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.electionTick.stop()
	rf.heartbeatTick.stop()
	log.Printf("%d dead\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) turnCandidate() {
	rf.state = candidate
	rf.votedFor = rf.me
	rf.vote = 1
	rf.currentTerm++
	log.Printf("%d turn candidate with term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) turnFollower() {
	log.Printf("%d turn follower with term %d\n", rf.me, rf.currentTerm)
	rf.state = follower
	rf.votedFor = -1
	rf.heartbeatTick.stop()
	rf.electionTick.start()
}

func (rf *Raft) turnLeader() {
	rf.state = leader
	for peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
	rf.electionTick.stop()
	rf.heartbeatTick.start()
	log.Printf("%d turn leader with term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) receiveVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.state != candidate || args.Term != rf.currentTerm {
		return
	}
	
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.turnFollower()
		rf.persist()
		return
	}
	
	if reply.VoteGranted {
		rf.vote++
		log.Printf("%d receive vote from %d, vote: %d", rf.me, peer, rf.vote)
	}
	
	if rf.vote > (len(rf.peers) / 2) {
		rf.turnLeader()
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if len(rf.peers) == 1 {
		rf.turnLeader()
		return
	}
	
	rf.turnCandidate()
	rf.persist()
	
	log.Printf("%d begin election\n", rf.me)
	
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		
		args := RequestVoteArgs {
			rf.currentTerm,
			rf.me,
			lastLogIndex,
			lastLogTerm,
		}
		reply := RequestVoteReply{}
		
		log.Printf("%d send RequestVote to %d\n", rf.me, peer)
		go rf.sendRequestVote(peer, &args, &reply)
	}
}

func (rf *Raft) checkCommit() {
	// TODO more effctive
	rf.matchIndex[rf.me] = len(rf.log) - 1
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	half := len(rf.peers) / 2
	
	if rf.commitIndex < matchIndex[half] &&
	   rf.currentTerm == rf.log[matchIndex[half]].Term {
		for i := rf.commitIndex + 1; i <= matchIndex[half]; i++ {
			log.Printf("[leader: %d, term: %d] apply command [%v] with index [%d]\n", rf.me, rf.currentTerm, rf.log[i].Command, i)
			rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
		}
		log.Printf("[leader: %d, term: %d] commit: [%d, %d]\n", rf.me, rf.currentTerm, rf.commitIndex + 1, matchIndex[half])
		rf.commitIndex = matchIndex[half]
	}
}

func (rf *Raft) hearbeatFeedback(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	log.Printf("[leader: %d, term: %d] receive AppendEntries feedback with [PrevLogIndex: %d, PrevLogTerm: %d, entries: %v] from [follower: %d, term: %d] \n", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.Entries, peer, reply.Term)
	
	if rf.state != leader || args.Term != rf.currentTerm {
		return
	}
	
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.turnFollower()
		rf.persist()
		return
	}
	
	if reply.Success {
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
	} else {
		rf.matchIndex[peer] = 0
//		rf.nextIndex[peer] = args.PrevLogIndex
		
		// fast backup
		if reply.XTerm == -1 {
			rf.nextIndex[peer] = reply.XLen
		} else {
			var xindex int
			// TODO binary search
			for xindex = args.PrevLogIndex;
			    xindex >= 1 && rf.log[xindex].Term != reply.XTerm;
			    xindex-- {}
			if xindex > 0 {
				rf.nextIndex[peer] = xindex
			} else {
				rf.nextIndex[peer] = reply.XIndex
			}
		}
		
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.Entries = nil
		
		log.Printf("[leader: %d, term: %d] re-send AppendEntries with [PrevLogIndex: %d, PrevLogTerm: %d, entries: %v] to [follower: %d]\n", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.Entries, peer)
		go rf.sendAppendEntries(peer, args, reply)
	}
}

func (rf *Raft) heartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	rf.checkCommit()
	
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		
		args := AppendEntriesArgs {
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: rf.nextIndex[peer] - 1,
			PrevLogTerm: rf.log[rf.nextIndex[peer] - 1].Term,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		
		if len(rf.log) > rf.nextIndex[peer] {
			if rf.matchIndex[peer] > 0 || args.PrevLogIndex == 0 {
				args.Entries = rf.log[rf.nextIndex[peer]:]
			}
			rf.nextIndex[peer]++
		}
		
		log.Printf("[leader: %d, term: %d] send AppendEntries with [PrevLogIndex: %d, PrevLogTerm: %d, entries: %v] to [follower: %d]\n", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.Entries, peer)
		go rf.sendAppendEntries(peer, &args, &reply)
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
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.electionTick = newTick(500, 150, rf.election, false)
	rf.heartbeatTick = newTick(100, 0, rf.heartbeats, true)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntries{nil, 0})
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	rf.turnFollower()
	rf.persist()

	return rf
}
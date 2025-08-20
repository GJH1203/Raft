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

import (
	"sync"
	"labrpc"
	"time"
	"math/rand"
	"bytes"
	"encoding/gob"
)



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Server states
const (
	Follower = iota
	Candidate
	Leader
)

// Log entry structure
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers (Figure 2)
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	log         []LogEntry // log entries; each entry contains command for state machine

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send
	matchIndex []int // for each server, index of highest log entry known to be replicated

	// Additional state
	state       int           // current state (Follower, Candidate, Leader)
	applyCh     chan ApplyMsg // channel to send committed entries
	lastHeartbeat time.Time   // time of last heartbeat received
	electionTimeout time.Duration // randomized election timeout
	killed      bool          // has this instance been killed?
	
	// Snapshot state
	snapshot          []byte // most recent snapshot data
	snapshotIndex     int    // index of log entry immediately preceding ones in snapshot
	snapshotTerm      int    // term of snapshotIndex entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// According to Figure 2, we need to persist:
	// currentTerm, votedFor, log[]
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistWithSnapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.persister.SaveSnapshot(rf.snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	if data == nil || len(data) < 1 { // bootstrap without any state
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	
	// Try to decode snapshot state (might not exist in older persisted data)
	if d.Decode(&rf.snapshotIndex) != nil {
		rf.snapshotIndex = 0
	}
	if d.Decode(&rf.snapshotTerm) != nil {
		rf.snapshotTerm = 0
	}
}




//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}
	
	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		reply.Term = rf.currentTerm
	}
	
	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		rf.persist()
	}
}

// Check if candidate's log is at least as up-to-date as this server's log
func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastIndex := rf.snapshotIndex + len(rf.log)
	myLastTerm := rf.snapshotTerm
	
	if len(rf.log) > 0 {
		myLastTerm = rf.log[len(rf.log)-1].Term
	}
	
	// Compare terms first, then indices
	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term         int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm int  // term of conflicting entry (for faster backup)
	ConflictIndex int // first index it stores for ConflictTerm
}

//
// InstallSnapshot RPC arguments structure.
//
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

//
// InstallSnapshot RPC reply structure.
//
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}
	
	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		reply.Term = rf.currentTerm
	}
	
	rf.state = Follower
	rf.lastHeartbeat = time.Now()
	
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.snapshotIndex {
		logIndex := args.PrevLogIndex - rf.snapshotIndex - 1
		if logIndex >= len(rf.log) {
			// Log is too short
			reply.ConflictIndex = rf.snapshotIndex + len(rf.log) + 1
			reply.ConflictTerm = -1
			return
		} else if rf.log[logIndex].Term != args.PrevLogTerm {
			// Conflicting term
			reply.ConflictTerm = rf.log[logIndex].Term
			// Find first index of conflicting term
			for i := 0; i < len(rf.log); i++ {
				if rf.log[i].Term == reply.ConflictTerm {
					reply.ConflictIndex = rf.snapshotIndex + i + 1
					break
				}
			}
			return
		}
	} else if args.PrevLogIndex == rf.snapshotIndex {
		// PrevLogIndex matches our snapshot index, check term
		if args.PrevLogTerm != rf.snapshotTerm {
			reply.ConflictIndex = rf.snapshotIndex
			reply.ConflictTerm = -1
			return
		}
	} else if args.PrevLogIndex >= 0 {
		// PrevLogIndex is before our snapshot, reject
		reply.ConflictIndex = rf.snapshotIndex + 1
		reply.ConflictTerm = -1
		return
	}
	
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	if len(args.Entries) > 0 {
		insertIndex := args.PrevLogIndex + 1
		logInsertIndex := insertIndex - rf.snapshotIndex - 1
		
		// Find conflicts and append new entries
		for i, entry := range args.Entries {
			currentLogIndex := logInsertIndex + i
			if currentLogIndex < len(rf.log) {
				if rf.log[currentLogIndex].Term != entry.Term {
					// Conflict found, truncate log from here
					rf.log = rf.log[:currentLogIndex]
					rf.log = append(rf.log, entry)
				}
				// Entry already exists and matches, skip
			} else {
				// Append new entry
				rf.log = append(rf.log, entry)
			}
		}
		rf.persist()
	}
	
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := rf.snapshotIndex + len(rf.log)
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		go rf.applyCommittedEntries()
	}
	
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.state != Leader {
		return -1, -1, false
	}
	
	// Append entry to local log
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	
	index := rf.snapshotIndex + len(rf.log) // 1-based indexing for external interface
	term := rf.currentTerm
	
	// Start replication to followers
	go rf.sendAppendEntriesToAll()
	
	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.killed = true
}

//
// Save snapshot and compact log
//
func (rf *Raft) SaveSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// Don't snapshot if index is older than our current snapshot
	if index <= rf.snapshotIndex {
		return
	}
	
	// Find the log entry at the given index
	logIndex := index - rf.snapshotIndex - 1
	if logIndex < 0 || logIndex >= len(rf.log) {
		return
	}
	
	rf.snapshotTerm = rf.log[logIndex].Term
	rf.snapshotIndex = index
	rf.snapshot = snapshot
	
	// Compact the log - keep entries after the snapshot
	rf.log = rf.log[logIndex+1:]
	
	// Update indices
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	
	// Save state with snapshot
	rf.persistWithSnapshot()
}

//
// InstallSnapshot RPC handler
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Term = rf.currentTerm
	
	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	
	// Update term and convert to follower if necessary
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}
	
	rf.lastHeartbeat = time.Now()
	
	// Return if we already have a more recent snapshot
	if args.LastIncludedIndex <= rf.snapshotIndex {
		return
	}
	
	// Create apply msg for service layer
	msg := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    args.Data,
		Index:       args.LastIncludedIndex,
	}
	
	// Discard log entries covered by snapshot
	oldSnapshotIndex := rf.snapshotIndex
	discardCount := args.LastIncludedIndex - oldSnapshotIndex
	if discardCount > 0 && discardCount <= len(rf.log) {
		rf.log = rf.log[discardCount:]
	} else {
		rf.log = make([]LogEntry, 0)
	}
	
	// Update snapshot state
	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	
	// Update indices
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	
	rf.persistWithSnapshot()
	
	// Send to service layer
	go func() {
		rf.applyCh <- msg
	}()
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

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = Follower
	rf.applyCh = applyCh
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.killed = false
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// Start background goroutines
	go rf.electionTicker()
	go rf.heartbeatTicker()

	return rf
}

// Background goroutine to check for election timeout
func (rf *Raft) electionTicker() {
	for {
		time.Sleep(10 * time.Millisecond)
		
		rf.mu.Lock()
		if rf.killed {
			rf.mu.Unlock()
			return
		}
		if rf.state != Leader && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// Background goroutine to send heartbeats when leader
func (rf *Raft) heartbeatTicker() {
	for {
		time.Sleep(100 * time.Millisecond)
		
		rf.mu.Lock()
		if rf.killed {
			rf.mu.Unlock()
			return
		}
		if rf.state == Leader {
			rf.sendAppendEntriesToAll()
		}
		rf.mu.Unlock()
	}
}

// Start an election
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.persist()
	
	currentTerm := rf.currentTerm
	votes := 1 // vote for self
	var mu sync.Mutex
	
	// Send RequestVote RPCs to all other servers
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				if rf.state != Candidate || rf.currentTerm != currentTerm {
					rf.mu.Unlock()
					return
				}
				
				lastLogIndex := rf.snapshotIndex + len(rf.log)
				lastLogTerm := rf.snapshotTerm
				if len(rf.log) > 0 {
					lastLogTerm = rf.log[len(rf.log)-1].Term
				}
				
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				rf.mu.Unlock()
				
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						rf.persist()
						return
					}
					
					if rf.state == Candidate && rf.currentTerm == currentTerm && reply.VoteGranted {
						mu.Lock()
						votes++
						if votes > len(rf.peers)/2 {
							rf.becomeLeader()
						}
						mu.Unlock()
					}
				}
			}(i)
		}
	}
}

// Become leader after winning election
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	
	for i := range rf.peers {
		rf.nextIndex[i] = rf.snapshotIndex + len(rf.log) + 1
		rf.matchIndex[i] = rf.snapshotIndex
	}
	
	// Send immediate heartbeat
	go rf.sendAppendEntriesToAll()
}

// Send AppendEntries to all followers
func (rf *Raft) sendAppendEntriesToAll() {
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				
				// Check if we need to send InstallSnapshot instead
				if rf.nextIndex[server] <= rf.snapshotIndex {
					args := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.snapshotIndex,
						LastIncludedTerm:  rf.snapshotTerm,
						Data:              rf.snapshot,
					}
					rf.mu.Unlock()
					
					reply := &InstallSnapshotReply{}
					if rf.sendInstallSnapshot(server, args, reply) {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.state = Follower
							rf.persist()
						} else if rf.state == Leader {
							rf.nextIndex[server] = rf.snapshotIndex + 1
							rf.matchIndex[server] = rf.snapshotIndex
						}
						rf.mu.Unlock()
					}
					return
				}
				
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := 0
				
				// Calculate actual index in our log array
				logIndex := prevLogIndex - rf.snapshotIndex - 1
				if prevLogIndex == rf.snapshotIndex {
					prevLogTerm = rf.snapshotTerm
				} else if logIndex >= 0 && logIndex < len(rf.log) {
					prevLogTerm = rf.log[logIndex].Term
				}
				
				entries := make([]LogEntry, 0)
				startIndex := rf.nextIndex[server] - rf.snapshotIndex - 1
				if startIndex >= 0 && startIndex < len(rf.log) {
					entries = rf.log[startIndex:]
				}
				
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntries(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						rf.persist()
						return
					}
					
					if rf.state == Leader {
						if reply.Success {
							rf.nextIndex[server] = rf.snapshotIndex + len(rf.log) + 1
							rf.matchIndex[server] = rf.snapshotIndex + len(rf.log)
							rf.updateCommitIndex()
						} else {
							// Use conflict information to quickly backup nextIndex
							if reply.ConflictTerm == -1 {
								// Follower's log is too short
								rf.nextIndex[server] = reply.ConflictIndex
							} else {
								// Find last entry with conflicting term in leader's log
								lastIndex := -1
								for i := len(rf.log) - 1; i >= 0; i-- {
									if rf.log[i].Term == reply.ConflictTerm {
										lastIndex = rf.snapshotIndex + i + 1
										break
									}
								}
								if lastIndex >= 0 {
									rf.nextIndex[server] = lastIndex + 1
								} else {
									rf.nextIndex[server] = reply.ConflictIndex
								}
							}
							rf.nextIndex[server] = max(rf.snapshotIndex+1, rf.nextIndex[server])
						}
					}
				}
			}(i)
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Update commit index based on majority replication
func (rf *Raft) updateCommitIndex() {
	for n := rf.snapshotIndex + len(rf.log); n > rf.commitIndex; n-- {
		count := 1 // count self
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		
		logIndex := n - rf.snapshotIndex - 1
		if count > len(rf.peers)/2 && logIndex >= 0 && logIndex < len(rf.log) && rf.log[logIndex].Term == rf.currentTerm {
			rf.commitIndex = n
			go rf.applyCommittedEntries()
			break
		}
	}
}

// Apply committed entries to state machine
func (rf *Raft) applyCommittedEntries() {
	rf.mu.Lock()
	
	msgs := make([]ApplyMsg, 0)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		logIndex := rf.lastApplied - rf.snapshotIndex - 1
		if logIndex >= 0 && logIndex < len(rf.log) {
			msg := ApplyMsg{
				Index:   rf.lastApplied, // 1-based indexing for external interface
				Command: rf.log[logIndex].Command,
			}
			msgs = append(msgs, msg)
		}
	}
	rf.mu.Unlock()
	
	// Send messages without holding the lock to avoid deadlock
	for _, msg := range msgs {
		rf.applyCh <- msg
	}
}

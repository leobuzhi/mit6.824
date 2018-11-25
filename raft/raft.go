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
	"math/rand"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/leobuzhi/mit6.824/labrpc"
)

const (
	stateLeader = iota
	stateCandidate
	stateFollower
)

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

type LogEntry struct {
	Index int
	Term  int
	Cmd   interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state      int
	votedCount int
	hearteat   chan struct{}
	leader     chan struct{}
	commit     chan struct{}
	apply      chan ApplyMsg

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == stateLeader
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//note(joey.chen): it must add lock,because the server maybe change  status
	//when other server send RequestVote RPC
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() { reply.Term = rf.currentTerm }()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = stateFollower
		rf.votedFor = -1
	}

	lastTerm := rf.getLastTerm()
	lastLogIndex := rf.getLastLogIndex()

	uptoDate := false
	if args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastLogIndex) {
		uptoDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && uptoDate {
		reply.VoteGranted = true

		rf.votedFor = args.CandidateID
		rf.state = stateFollower
		rf.hearteat <- struct{}{}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != stateCandidate || rf.currentTerm != args.Term {
			return false
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = stateFollower
			rf.votedFor = -1
		}
		if reply.VoteGranted {
			rf.votedCount++
			if rf.votedCount > len(rf.peers)/2 {
				rf.state = stateLeader
				rf.leader <- struct{}{}
			}
		}
	}
	return true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == stateLeader

	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.logs = append(rf.logs, LogEntry{Index: index, Term: term, Cmd: command})
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) broadcastRequestVote() {
	if rf.state != stateCandidate {
		glog.Warning("broadcastRequestVote but not candidate!!!")
		return
	}

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogTerm: rf.getLastTerm(), LastLogIndex: rf.getLastLogIndex()}

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.sendRequestVote(i, &args, new(RequestVoteReply))
			}(i)
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.logs[0].Index
	n := rf.commitIndex
	for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.logs[i-index].Term == rf.currentTerm {
				num++
			}
		}
		if 2*num > len(rf.peers) {
			n = i
		}
	}
	if n != rf.commitIndex {
		rf.commitIndex = n
		rf.commit <- struct{}{}
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == stateLeader {
			if rf.nextIndex[i] > index {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.PreLogIndex = rf.nextIndex[i] - 1
				args.PreLogTerm = rf.logs[args.PreLogIndex-index].Term
				args.LeaderCommit = rf.commitIndex
				args.Logs = make([]LogEntry, len(rf.logs[args.PreLogIndex+1-index:]))
				copy(args.Logs, rf.logs[args.PreLogIndex+1-index:])

				go func(i int, args AppendEntriesArgs) {
					rf.sendAppendEntries(i, args, new(AppendEntriesReply))
				}(i, args)
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	LeaderCommit int
	Logs         []LogEntry
	PreLogIndex  int
	PreLogTerm   int
}

type AppendEntriesReply struct {
	Success   bool
	Term      int
	NextIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}

	//to tell others someone become leader
	rf.hearteat <- struct{}{}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = stateFollower
		rf.votedFor = -1
	}
	reply.Term = args.Term

	if args.PreLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}

	index := rf.logs[0].Index
	if args.PreLogIndex > index {
		term := rf.logs[args.PreLogIndex-index].Term
		if args.PreLogTerm != term {
			for i := args.PreLogIndex - 1; i >= index; i-- {
				if rf.logs[i-index].Term != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}

	if args.PreLogIndex >= index {
		rf.logs = rf.logs[:args.PreLogIndex+1-index]
		rf.logs = append(rf.logs, args.Logs...)
		reply.NextIndex = rf.getLastLogIndex() + 1
		reply.Success = true
	}

	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastLogIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commit <- struct{}{}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state != stateLeader || rf.currentTerm != args.Term {
		return
	}

	if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = stateFollower
			rf.votedFor = -1
			return
		}

		if reply.Success {
			if len(args.Logs) > 0 {
				rf.nextIndex[server] = args.Logs[len(args.Logs)-1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
}

//reference(joey.chen): http://nil.csail.mit.edu/6.824/2016/papers/raft-extended.pdf
func timeWithoutLeader() time.Duration {
	return time.Duration(rand.Int()%150+150) * time.Millisecond
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
	rf.state = stateFollower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.apply = applyCh
	rf.logs = append(rf.logs, LogEntry{})
	rf.hearteat = make(chan struct{}, 10)
	rf.leader = make(chan struct{}, len(peers))
	rf.commit = make(chan struct{}, 10)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case stateFollower:
				select {
				case <-rf.hearteat:
				case <-time.After(timeWithoutLeader()):
					rf.state = stateCandidate
				}
			case stateLeader:
				rf.broadcastAppendEntries()
				//reference(joey.chen): broadcastTime ≪ electionTimeout ≪ MTBF
				//MTBF is the average time between failures for a single server.
				time.Sleep(5 * time.Millisecond)
			case stateCandidate:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = me
				rf.votedCount = 1
				rf.mu.Unlock()

				go rf.broadcastRequestVote()

				select {
				case <-time.After(timeWithoutLeader()):
				case <-rf.hearteat: //others become leader
					rf.state = stateFollower
				case <-rf.leader: //become leader
					rf.mu.Lock()
					rf.state = stateLeader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.commit:
				rf.mu.Lock()
				index := rf.logs[0].Index
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					applyCh <- ApplyMsg{CommandIndex: i, Command: rf.logs[i-index].Cmd, CommandValid: true}
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}

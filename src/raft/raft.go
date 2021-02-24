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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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

var HeartBeatInterval = 10 * time.Millisecond

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
	role RoleType // 0-leader, 1-follower or 2-candidate

	// persistent state on all servers
	currentTerm int     // latest term server has seen
	votedFor    int     // candidateID that received vote in current term
	log         []Entry // log entries

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// timer
	electionTimer *time.Timer
	pingTimer     *time.Timer

	applyCh chan ApplyMsg
}

type RoleType int

const (
	LEADER    RoleType = 0
	FOLLOWER  RoleType = 1
	CANDIDATE RoleType = 2
)

func (r RoleType) String() string {
	switch r {
	case LEADER:
		return "Leader"
	case FOLLOWER:
		return "Follower"
	case CANDIDATE:
		return "Candidate"
	default:
		return "Unknown"
	}
}

type Entry struct {
	Term    int // 这条日志所属的Term
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
	// return term, isleader
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
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("[Candidate %v; Term %v; LastLogIndex %v; LastLogTerm %v]",
		args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)
}

type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderID     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store
	LeaderCommit int     // leader's commit index
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// _, _ = DPrintf("Call %v RequestVote. Args: %v", rf.String(), args)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// candidate 的 term 落后于 follower
		_, _ = DPrintf("candidate term is smaller than follower %v", rf.String())
		return
	}

	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.electionTimer.Reset(getRandomElectionTimeout())
		_, _ = DPrintf("%v change to candidate", rf.String())
	}

	// candidate term >= follower's term
	// _, _ = DPrintf("Request Vote. Follower %v vote for %v", rf.String(), rf.votedFor)
	if rf.votedFor == args.CandidateID || rf.votedFor == -1 {
		// candidate's log is at least up up-to-date as receiver's log
		// _, _ = DPrintf("Follower %v hasn't vote yet.", rf.String())
		/*
			Raft determines which of two logs is more up-to-date by
			comparing the index and term of the last entries in the logs.

			If the logs have last entries with different terms,
			then the log with the later term is more up-to-date.

			If the logs end with the same term, then whichever log is longer is more up-to-date.
		*/

		// _, _ = DPrintf("args.LastLogTerm: %v. rf.log[len(rf.log)-1].Term: %v", args.LastLogTerm, rf.log[len(rf.log)-1].Term)
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.electionTimer.Reset(getRandomElectionTimeout())
			_, _ = DPrintf("%v Vote for %v", rf.String(), args.CandidateID)
			return
		} else if rf.log[len(rf.log)-1].Term == args.LastLogTerm {
			if args.LastLogIndex >= len(rf.log)-1 {
				reply.Term = args.Term
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
				rf.electionTimer.Reset(getRandomElectionTimeout())
				_, _ = DPrintf("%v Vote for %v", rf.String(), args.CandidateID)
				return
			}
		}
	}
	_, _ = DPrintf("Request Vote Failed. Follower vote for %v", rf.votedFor)
	reply.Term = args.Term
	reply.VoteGranted = false
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// _, _ = DPrintf("Call AppendEntry. args: %v", args)

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		// leader 的 term 落后于 follower
		_, _ = DPrintf("leader's %v term is smaller than follower %v", args, rf.String())
		return
	}

	// 在等待投票期间, candidate可能会收到另一个声称自己是leader的服务器节点发来的 AppendEntries RPC.
	// 如果这个leader的任期号不小于candidate当前的任期号, 那么 candidate 会承认该 leader 的合法地位
	rf.role = FOLLOWER
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.electionTimer.Reset(getRandomElectionTimeout())

	if len(rf.log) <= args.PrevLogIndex {
		// log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		// follower 在它的日志中找不到包含相同索引位置和任期号的条目，那么他就会拒绝该新的日志条目
		_, _ = DPrintf("log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm")
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// an existing entry conflicts with a new one (same index but different terms),
		_, _ = DPrintf("conflict with new one")
		// deleting the existing
		rf.log = rf.log[0:args.PrevLogIndex]
		return
	}

	//_, _ = DPrintf("AppendEntry...")
	// append any new entries not already in the log
	rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		// commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = true
	// _, _ = DPrintf("Send AppendEntry Successfully")
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
	// _, _ = DPrintf("Send RequestVote. reply: %v", reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	// Raft初始化过程
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.votedFor = -1
	rf.mu = sync.Mutex{}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.role = FOLLOWER
	rand.Seed(time.Now().UnixNano())
	rf.log = []Entry{
		{
			Term:    0,
			Command: nil,
		},
	}

	rf.electionTimer = time.NewTimer(getRandomElectionTimeout())
	rf.pingTimer = time.NewTimer(HeartBeatInterval)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionLoop()
	go rf.applyLoop()

	return rf
}

//
// 三个loop: electionLoop, pingLoop, applyLoop
//
func (rf *Raft) electionLoop() {
	for {
		<-rf.electionTimer.C
		rf.electionTimer.Reset(getRandomElectionTimeout())

		// 如果自己是leader就不重新选举
		if _, isLeader := rf.GetState(); isLeader {
			continue
		}

		// 变成候选人
		rf.mu.Lock()
		rf.role = CANDIDATE
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		_, _ = DPrintf("%s change to candidate", rf.String())
		//rf.mu.Unlock()

		// 请求投票
		// 保证当选的 candidate 的 log 比过半数的 peer 更 up-to-date


		// wg := sync.WaitGroup{}
		cond := sync.NewCond(&rf.mu)
		vote := 0
		finish := 0
		for i := range rf.peers {

			if i == rf.me {
				vote++
				finish++
				continue
			}

			// wg.Add(1)
			go func(id int) {
				//rf.mu.Lock()
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				reply := &RequestVoteReply{}
				//rf.mu.Unlock()

				_, _ = DPrintf("Send Request index %v Vote. args: %v", id, args)
				ok := rf.sendRequestVote(id, args, reply)
				// fmt.Printf("ok: %v\n", ok)

				defer func() {
					// wg.Done()
					// fmt.Printf("waitgroup done. id:%v\n", id)
					finish++
					cond.Broadcast()
				}()

				//rf.mu.Lock()
				//defer rf.mu.Unlock()

				if !ok {
					_, _ = DPrintf("%v get request vote reply from peer-%d: %v", rf.String(), id, reply)
					return
				}

				if reply.VoteGranted {
					vote++
				}

				if reply.Term > rf.currentTerm {
					// 变回 follower
					rf.role = FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.electionTimer.Reset(getRandomElectionTimeout())
				}

				if rf.role != CANDIDATE || rf.currentTerm != args.Term {
					return
				}
			}(i)
			// wg.Done()
		}

		// if vote <= len(rf.peers) / 2 {
			// wg.Wait()
		// }

		// wg.Wait()
		for vote <= len(rf.peers) / 2 && finish != len(rf.peers) {
			//fmt.Printf("finished: %v\n", finish)
			cond.Wait()
		}


		_, _ = DPrintf("%v Received %v tickets.", rf.String(), vote)

		if vote > len(rf.peers) / 2 {
			// 获得过半选票，成为 leader
			if rf.role != CANDIDATE {
				return
			}

			rf.role = LEADER
			rf.votedFor = -1
			for i := range rf.peers {
				rf.matchIndex[i] = 0
				rf.nextIndex[i] = len(rf.log)
			}
			rf.pingTimer.Reset(HeartBeatInterval)
			go rf.pingLoop()

			_, _ = DPrintf("%s change to leader", rf.String())

		}
		rf.mu.Unlock()
	}


}

func (rf *Raft) pingLoop() {
	for {

		// 如果不是 leader 直接返回
		if _, isLeader := rf.GetState(); !isLeader {
			return
		}

		<-rf.pingTimer.C

		for i := range rf.peers {
			if i == rf.me {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = len(rf.log) - 1
				continue
			}

			go func(id int) {

				for {
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						PrevLogIndex: rf.nextIndex[id] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[id]-1].Term,
						Entries:      rf.log[rf.nextIndex[id]:],
						LeaderCommit: rf.commitIndex,
					}

					reply := &AppendEntriesReply{}
					// _, _ = DPrintf("%v Send AppendEntries. Args: %v", rf.String(), args)
					if ok := rf.sendAppendEntries(id, args, reply); !ok {
						// _, _ = DPrintf("%v Send AppendEntries RPC Failed. Args: %v", rf.String(), args)
						return
					}

					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						// 变回 follower
						rf.currentTerm = reply.Term
						rf.role = FOLLOWER
						rf.votedFor = -1
						rf.electionTimer.Reset(getRandomElectionTimeout())
						_, _ = DPrintf("%v change to follower.", rf.String())
						rf.mu.Unlock()
						return
					}

					if rf.currentTerm != args.Term || rf.role != LEADER {
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[id] = rf.matchIndex[id] + 1

						rf.mu.Unlock()
						break
					} else {
						//for args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
						//	rf.nextIndex[id]
						//}
						// _, _ = DPrintf("Retry AppendEntries")
						if args.PrevLogIndex > 0 {
							rf.nextIndex[id]--
						}
						rf.mu.Unlock()
					}
				}
			}(i)
		}

		rf.pingTimer.Reset(HeartBeatInterval)
	}
}

func (rf *Raft) applyLoop() {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
		}
		rf.mu.Unlock()
	}
}

func getRandomElectionTimeout() time.Duration {
	return time.Duration((rand.Intn(150) + 150) * int(time.Millisecond))
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[Role:%v; Index:%d; Term:%d; VotedFor:%d; logLen:%v; Commit:%v; Apply:%v; Peer:%v]",
		rf.role, rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.commitIndex, rf.lastApplied, len(rf.peers))
}

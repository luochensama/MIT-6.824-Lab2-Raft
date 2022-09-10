package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type SnapShot struct {
	Data             []byte
	LastIncludeIndex int
	LastIncludeTerm  int
}

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	snapShot                 SnapShot
	tempSnapShot             SnapShot
	timeout                  bool // 如果收到心跳的话置为false
	limit                    int
	state                    State
	currentTerm              int
	votedFor                 int
	log                      []LogEntry
	commitIndex              int
	lastApplied              int
	nextIndex                []int
	matchIndex               []int
	nAccept                  []int // 需要一个数组来判断是否大多数服务器都接受了这个Log
	currentTermFirstLogIndex int   // 当前term中第一个日志的索引
	applyMsg                 chan ApplyMsg
	applyCond                *sync.Cond
	sendEntriesCond          *sync.Cond
	encoder                  *labgob.LabEncoder
	buffer                   *bytes.Buffer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	res := rf.mu.TryLock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.snapShot.LastIncludeIndex)
	e.Encode(rf.snapShot.LastIncludeTerm)
	e.Encode(rf.log)
	data := w.Bytes()

	rf.persister.SaveRaftState(data)
	if res {
		rf.mu.Unlock()
	}
}

func (rf *Raft) persistStateAndSnapShot() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	res := rf.mu.TryLock()
	DPrintf("start")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.snapShot.LastIncludeIndex)
	e.Encode(rf.snapShot.LastIncludeTerm)
	e.Encode(rf.log)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, rf.snapShot.Data)

	if res {
		rf.mu.Unlock()
	}
	DPrintf("done")
}

func (rf *Raft) readPersist(data []byte) {
	DPrintf("bug1!")
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	snapr := bytes.NewBuffer(rf.persister.ReadSnapshot())
	snapd := labgob.NewDecoder(snapr)

	var votedFor, currentTerm, lastIncludeIndex, lastIncludeTerm int
	var log []LogEntry
	var snap []byte
	if d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil || d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil || d.Decode(&log) != nil {
		DPrintf("bug2!")
	} else {
		res := rf.mu.TryLock()
		rf.votedFor = votedFor
		rf.log = log
		rf.currentTerm = currentTerm
		rf.snapShot.LastIncludeIndex = lastIncludeIndex
		rf.snapShot.LastIncludeTerm = lastIncludeTerm
		if snapd.Decode(&snap) == nil {
			rf.snapShot.Data = snap
		}
		DPrintf("log: %v", log)

		if res {
			rf.mu.Unlock()
		}
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()

	index--
	DPrintf("server %d do snapshot index:%d\n log: %v", rf.me, index, rf.log[:index-rf.snapShot.LastIncludeIndex])

	if rf.state == Leader { //leader还要修改nextIndex的内容
		length := len(rf.log[rf.getLogIndex(index+1):])
		for i := 0; i < len(rf.nextIndex); i++ {
			if rf.nextIndex[i] == len(rf.log) { // 如果没得发，之后也没得发
				rf.nextIndex[i] = length
			} else {
				rf.nextIndex[i] = length - 1 // 否则就从尾部发，就不分类讨论了
			}
		}
	}

	term := rf.log[rf.getLogIndex(index)].Term
	rf.log = rf.log[rf.getLogIndex(index+1):] // 丢掉index以及之前的日志
	if rf.state == Leader {
		firstIndex := -1
		for key, val := range rf.log {
			if val.Term == rf.currentTerm {
				firstIndex = key
				break
			}
		}
		if firstIndex == -1 {
			rf.nAccept = make([]int, 0)
		} else {
			globalOldIndex := rf.getGlobalIndex(rf.currentTermFirstLogIndex)
			globalNewIndex := firstIndex + index + 1

			rf.nAccept = rf.nAccept[globalNewIndex-globalOldIndex:]
		}
		rf.currentTermFirstLogIndex = firstIndex
	}
	rf.commitIndex = rf.getGlobalIndex(rf.commitIndex)
	rf.lastApplied = rf.getGlobalIndex(rf.lastApplied)

	rf.snapShot = SnapShot{
		Data:             snapshot,
		LastIncludeIndex: index,
		LastIncludeTerm:  term,
	}

	rf.commitIndex = rf.getLogIndex(rf.commitIndex)
	rf.lastApplied = rf.getLogIndex(rf.lastApplied)

	rf.persistStateAndSnapShot()

	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	XTerm   int
	XIndex  int // index of first entry where term = XTerm
	XLen    int
	Success bool
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Offset           int
	Data             []byte
	Done             bool
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { // 过时的消息
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm { // 自己可能落后了
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	lastLogIndex := len(rf.log) - 1
	var lastLogTerm int
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	} else {
		lastLogTerm = rf.snapShot.LastIncludeTerm
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // 还未投票或者投给了这个请求者

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && rf.getLogIndex(args.LastLogIndex) >= lastLogIndex) {
			DPrintf("%d index:%d term:%d log:%v", rf.me, lastLogIndex, lastLogTerm, rf.log)
			rf.votedFor = args.CandidateId
			rf.timeout = false
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.persist()
		} else {
			reply.Term = args.Term
			reply.VoteGranted = false
		}

	} else {

		reply.Term = args.Term
		reply.VoteGranted = false

	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.timeout = false
	reply.Term = rf.currentTerm

	rf.tempSnapShot.Data = args.Data
	rf.tempSnapShot.LastIncludeTerm = args.LastIncludeTerm
	rf.tempSnapShot.LastIncludeIndex = args.LastIncludeIndex

	if rf.getLogIndex(args.LastIncludeIndex) < len(rf.log) && rf.getLogIndex(args.LastIncludeIndex) >= 0 && rf.log[rf.getLogIndex(args.LastIncludeIndex)].Term == args.LastIncludeTerm { //匹配上了的情况一般应该是不会发生的？
		rf.log = rf.log[rf.getLogIndex(args.LastIncludeIndex)+1:]
	} else {
		rf.log = make([]LogEntry, 0)
		rf.lastApplied = -1
		rf.commitIndex = -1

		msg := ApplyMsg{
			CommandValid:  false,
			Snapshot:      rf.tempSnapShot.Data,
			SnapshotValid: true,
			SnapshotIndex: rf.tempSnapShot.LastIncludeIndex + 1,
			SnapshotTerm:  rf.tempSnapShot.LastIncludeTerm,
		}
		//
		rf.snapShot = rf.tempSnapShot

		rf.persistStateAndSnapShot()

		DPrintf("server %d install done log:%v lastindex:%d", rf.me, rf.log, rf.snapShot.LastIncludeIndex)

		rf.mu.Unlock()

		rf.applyMsg <- msg

		rf.mu.Lock()
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
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
var n1, n2, n3 int

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		isLeader = false
	} else {
		if rf.state == Leader {
			term = rf.currentTerm
			index = len(rf.log)

			if rf.currentTermFirstLogIndex == -1 {
				rf.currentTermFirstLogIndex = index
			}

			rf.log = append(rf.log, LogEntry{
				Term:    term,
				Command: command,
			})
			rf.persist()
			DPrintf("start:%v", rf.log)

			rf.nAccept = append(rf.nAccept, 1)

			for i := 0; i < len(rf.nextIndex); i++ {
				rf.nextIndex[i] = len(rf.log) - 1
			}

			index = rf.getGlobalIndex(index)

			//			go rf.syncFollowersLog()
			rf.sendEntriesCond.Broadcast()

		} else {
			isLeader = false
		}
	}

	return index + 1, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		sleepTime := rand.Intn(400) + 400 // 400~800 ms // 保证能收到心跳，目前的策略导致有一个票一直收不到的话会gg
		//		DPrintf("sleep time %d ms", sleepTime)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		//		DPrintf("!server %d wakeup try to elect leader", rf.me)
		rf.tryElectLeader()
	}

}

func (rf *Raft) tryElectLeader() {
	rf.mu.Lock()
	if rf.timeout == true && rf.state == Follower {

		DPrintf("server %d start vote round %d", rf.me, rf.currentTerm+1)
		rf.state = Candidate
		rf.votedFor = rf.me
		rf.currentTerm++
		rf.persist()

		currentTerm := rf.currentTerm
		candidateId := rf.me
		lastLogIndex := len(rf.log) - 1

		var lastLogTerm int
		if lastLogIndex >= 0 {
			lastLogTerm = rf.log[lastLogIndex].Term
		} else {
			lastLogTerm = rf.snapShot.LastIncludeTerm
		}

		rf.mu.Unlock()

		voted := 1
		wg := sync.WaitGroup{}

		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}

			wg.Add(1)

			go func(server int) {
				defer wg.Done()
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  candidateId,
					LastLogIndex: rf.getGlobalIndex(lastLogIndex),
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				res := rf.sendRequestVote(server, &args, &reply)

				// 投票可能是延迟很久的
				if res != true { // 可能调用超时了，那么就直接return就可以
					DPrintf("server %d vote rpc timeout ", rf.me)
					return
				}

				rf.mu.Lock()

				defer rf.mu.Unlock()

				if rf.currentTerm != currentTerm || rf.state != Candidate { // 过时的投票响应，或者已经成为了leader
					return
				} else if reply.Term > currentTerm { // 自身已经落后了

					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()

				} else if reply.VoteGranted == true { // 收到了投票

					voted++
					DPrintf("%d Got Vote From %d voted total %d", rf.me, server, voted)
					if voted > rf.limit { // 获得了大多数的支持，初始化一下相关数据结构
						rf.state = Leader
						rf.nAccept = make([]int, 0)
						rf.nextIndex = make([]int, len(rf.peers)) // 初始化为当前最大日志+1
						rf.matchIndex = make([]int, len(rf.peers))

						for i := 0; i < len(rf.nextIndex); i++ {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = -1
						}
						rf.currentTermFirstLogIndex = -1
						DPrintf("%d be the leader", rf.me)
						go rf.startHeartbeatLoop() // 开启定时发送心跳给Follower
						go rf.syncFollowersLog()   //  同步日志
					}
				}
			}(server)
		}

		wg.Wait()

		if voted <= rf.limit { // 需要处理竞选失败的情况。
			rf.mu.Lock()
			DPrintf("server %d tryElectLeader false", rf.me)
			defer rf.mu.Unlock()
			if currentTerm == rf.currentTerm && rf.state == Candidate {
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
			}
		}

	} else {
		rf.timeout = true
		rf.mu.Unlock()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { // 收到过时的消息
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.state != Follower { // 不是Follower却收到了信息，说明已经选举出了Leader
		rf.state = Follower
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term // 更新自己的term
		rf.votedFor = -1
		rf.state = Follower

		rf.persist()
	}

	rf.timeout = false // 刷新一下表明收到了心跳消息

	prevLogIndex := rf.getLogIndex(args.PrevLogIndex)
	globalPrevLogIndex := args.PrevLogIndex

	reply.Term = rf.currentTerm
	if args.Entries == nil { // 心跳包
		if globalPrevLogIndex == -1 || (prevLogIndex < len(rf.log) && prevLogIndex >= 0 && args.PrevLogTerm == rf.log[prevLogIndex].Term) {

			if rf.getLogIndex(args.LeaderCommit) > rf.commitIndex {
				if prevLogIndex > rf.commitIndex {
					rf.commitIndex = prevLogIndex
				}

				if rf.commitIndex > rf.lastApplied {
					rf.applyCond.Broadcast()
				}
			}

		}

		return
	}

	reply.XLen = rf.getGlobalIndex(len(rf.log))
	DPrintf("server %d recieve log:%v leader Commit %d client log: %v prevLogIndex:%d\nmy term:%d server term:%d", rf.me, args.Entries, args.LeaderCommit, rf.log, args.PrevLogIndex, rf.currentTerm, args.Term)
	if prevLogIndex > len(rf.log)-1 { // 超过了拥有的log长度，说明需要之前的
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		return
	} else if globalPrevLogIndex <= rf.snapShot.LastIncludeIndex { // 快照之前的log，直接加入到Log中就可以了
		if len(args.Entries)+globalPrevLogIndex > rf.snapShot.LastIncludeIndex {
			rf.log = append(rf.log[:0], args.Entries[rf.snapShot.LastIncludeIndex-globalPrevLogIndex:]...)
			rf.persist()
		}
		DPrintf("server %d accept log from server %d: %v", rf.me, args.LeaderId, args.Entries)

		reply.Success = true

	} else if args.PrevLogTerm != rf.log[prevLogIndex].Term { // 不匹配
		reply.Success = false
		reply.XTerm = rf.log[prevLogIndex].Term
		var index int
		for i := prevLogIndex; i >= 0; i-- {
			if i == 0 || rf.log[i-1] != rf.log[i] {
				index = i
			}
		}
		reply.XIndex = rf.getGlobalIndex(index)
		return
	} else { // 匹配上了，加入Log
		reply.Success = true
		if prevLogIndex+len(args.Entries) < len(rf.log) {
			return
		}
		rf.log = append(rf.log[:prevLogIndex+1], args.Entries...)
		DPrintf("server %d accept log from server %d: %v leader commit:%d", rf.me, args.LeaderId, args.Entries, args.LeaderCommit)

		rf.persist()
	}

	if args.LeaderCommit > rf.getGlobalIndex(rf.commitIndex) {
		if rf.commitIndex < rf.getLogIndex(args.PrevLogIndex) {
			rf.commitIndex = rf.getLogIndex(args.PrevLogIndex)
		}

		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Broadcast()
		}
	}

}

func (rf *Raft) syncFollowersLog() {
	for server, _ := range rf.peers {

		if server == rf.me {
			continue
		}

		go func(server int) {
			for {
				rf.mu.Lock()

				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				for rf.nextIndex[server] >= len(rf.log) { // 没有需要发送的Log就一直睡眠
					rf.sendEntriesCond.Wait()
				}

				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[server] == -1 { // 需要发送快照
					term := rf.currentTerm
					leaderId := rf.me
					lastIncludeIndex := rf.snapShot.LastIncludeIndex
					lastIncludeTerm := rf.snapShot.LastIncludeTerm

					args := InstallSnapshotArgs{
						Term:             term,
						LeaderId:         leaderId,
						LastIncludeIndex: lastIncludeIndex,
						LastIncludeTerm:  lastIncludeTerm,
						Offset:           0,
						Data:             rf.snapShot.Data,
						Done:             true,
					}

					reply := InstallSnapshotReply{}
					rf.mu.Unlock()

					res := rf.sendInstallSnapshot(server, &args, &reply)

					rf.mu.Lock()
					if res == false { // 超时了，需要重发
						rf.mu.Unlock()
						continue
					}

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.persist()
						rf.mu.Unlock()

						return
					}
					rf.nextIndex[server] = 0
					rf.matchIndex[server] = -1

					rf.mu.Unlock()

					continue
				}

				term := rf.currentTerm
				leaderId := rf.me

				leaderCommit := rf.getGlobalIndex(rf.commitIndex)

				prevLogIndex := rf.nextIndex[server] - 1
				globalPrevLogIndex := rf.getGlobalIndex(prevLogIndex)
				preLogTerm := 0
				var entries []LogEntry

				if prevLogIndex >= 0 {
					preLogTerm = rf.log[prevLogIndex].Term
				} else {
					preLogTerm = rf.snapShot.LastIncludeTerm
				}
				entries = append(entries, rf.log[prevLogIndex+1:]...)

				DPrintf("server %d send entries : %v to server %d nextIndex %d commitIndex:%d", rf.me, entries, server, rf.nextIndex[server], rf.commitIndex)

				rf.mu.Unlock()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     leaderId,
					PrevLogIndex: globalPrevLogIndex,
					PrevLogTerm:  preLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}

				reply := AppendEntriesReply{}
				m := sync.Mutex{} // lock for flag
				flag := -1
				go func() {
					res := rf.sendAppendEntries(server, &args, &reply)
					m.Lock()
					if res {
						flag = 1
					} else {
						flag = 0
					}
					m.Unlock()
				}()
				time.Sleep(30 * time.Millisecond) // 等一会结果
				m.Lock()
				if flag == -1 { // 没有结果需要重新发送
					m.Unlock()
					time.Sleep(300 * time.Millisecond) // 再睡久一点点
					m.Lock()
					if flag == -1 {
						m.Unlock() // 还是没结果就不等了，重新发一份
						continue
					}
				}

				if flag == 0 { // rpc超时，有可能是宕机了，慢点发
					DPrintf("server %d appendentries timeout", rf.me)
					m.Unlock()
					continue
				}
				m.Unlock()

				rf.mu.Lock()

				if reply.Success == false { // false说明两者日志不匹配，需要同步
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.persist()
						rf.mu.Unlock()
						return
					}

					if rf.getGlobalIndex(rf.nextIndex[server]) == globalPrevLogIndex+1 { // 如果还是之前的nextIndex
						if rf.nextIndex[server] > 0 {
							if reply.XTerm == -1 { // 需要之前的或者是需要快照
								if rf.getLogIndex(reply.XLen) < 0 { // 需要快照
									rf.nextIndex[server] = -1
								} else {
									rf.nextIndex[server] = rf.getLogIndex(reply.XLen)
								}
							} else {
								if rf.getLogIndex(reply.XIndex) < 0 {
									rf.nextIndex[server] = 0
								} else if rf.log[rf.getLogIndex(reply.XIndex)].Term == reply.XTerm { // 说明有该term的记录
									index := rf.getLogIndex(reply.XIndex) + 1
									for ; index < len(rf.log); index++ {
										if rf.log[index].Term != reply.XTerm {
											break
										}
									}
									rf.nextIndex[server] = index
								} else {
									rf.nextIndex[server] = rf.getLogIndex(reply.XIndex) // 说明压根没有该term记录
								}
							}
						} else { // 需要快照之前的index
							rf.nextIndex[server] = -1
						}
					}
				} else { // 同步成功
					if rf.currentTermFirstLogIndex != -1 {
						for i := 0; i < len(entries); i++ {
							logPos := rf.getLogIndex(i + globalPrevLogIndex + 1)
							acceptPos := logPos - rf.currentTermFirstLogIndex
							if acceptPos < 0 { // 当前term之前的日志不能参与当前term
								continue
							}

							rf.nAccept[acceptPos]++
							if rf.nAccept[acceptPos] == rf.limit+1 {
								if logPos > rf.commitIndex {
									rf.commitIndex = logPos
								}
								if rf.commitIndex > rf.lastApplied {
									rf.applyCond.Broadcast()
								}
							}
						}
					}

					rf.matchIndex[server] = rf.getLogIndex(globalPrevLogIndex + len(entries))

					if rf.nextIndex[server] == rf.getLogIndex(globalPrevLogIndex+1) { // 如果还是之前的
						rf.nextIndex[server] = rf.matchIndex[server] + 1
					}
				}
				rf.mu.Unlock()
			}
		}(server)
	}
}

func (rf *Raft) startHeartbeatLoop() { // 必须分割heartbeat和同步。否则很麻烦
	DPrintf("%d start heartbeatLoop", rf.me)

	for {
		rf.mu.Lock()

		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}

		rf.mu.Unlock()
		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()

				term := rf.currentTerm
				leaderId := rf.me
				leaderCommit := rf.getGlobalIndex(rf.commitIndex)
				prevLogIndex := rf.nextIndex[server] - 1
				preLogTerm := 0

				if prevLogIndex >= 0 {
					preLogTerm = rf.log[prevLogIndex].Term
				} else {
					preLogTerm = rf.snapShot.LastIncludeTerm
				}

				globalPrevLogIndex := rf.getGlobalIndex(prevLogIndex)

				var entries []LogEntry
				//				entries = append(entries, rf.log[prevLogIndex+1:]...)
				//				DPrintf("server %d send entries : %v to server %d", rf.me, entries, server)

				rf.mu.Unlock()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     leaderId,
					PrevLogIndex: globalPrevLogIndex,
					PrevLogTerm:  preLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}

				reply := AppendEntriesReply{}

				res := rf.sendAppendEntries(server, &args, &reply)

				if res != true { // rpc超时
					return
				}

				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
				}

				rf.mu.Unlock()

			}(server)
		}

		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) applyLog() {
	for rf.killed() == false {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		for rf.lastApplied < rf.commitIndex && rf.lastApplied+1 <= len(rf.log)-1 {
			rf.lastApplied++
			DPrintf("server %d apply log %d to app, leaderCommit %d", rf.me, rf.lastApplied, rf.commitIndex)
			command := rf.log[rf.lastApplied].Command
			commandIndex := rf.getGlobalIndex(rf.lastApplied + 1)

			rf.mu.Unlock()

			rf.applyMsg <- ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: commandIndex,
			}

			rf.mu.Lock()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) getGlobalIndex(index int) int {
	if index == -1 {
		return rf.snapShot.LastIncludeIndex
	}
	//	fmt.Println(rf.snapShot.LastIncludeIndex)
	return index + rf.snapShot.LastIncludeIndex + 1
}

func (rf *Raft) getLogIndex(index int) int {
	if index == -1 {
		return rf.snapShot.LastIncludeIndex
	}
	//	fmt.Println(rf.snapShot.LastIncludeIndex)
	return index - rf.snapShot.LastIncludeIndex - 1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.sendEntriesCond = sync.NewCond(&rf.mu)
	rf.dead = 0
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.applyMsg = applyCh
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.buffer = new(bytes.Buffer)
	rf.encoder = labgob.NewEncoder(rf.buffer)
	rf.snapShot.LastIncludeIndex = -1
	rf.snapShot.LastIncludeTerm = -1

	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = -1
	}

	rf.limit = len(peers) / 2

	// initialize from state persisted before a crash
	DPrintf("server %d connected", rf.me)
	rf.readPersist(persister.ReadRaftState())

	//	DPrintf("server %d currentTerm:%d,votedFor:%d,log:%v", rf.me, rf.currentTerm, rf.votedFor, rf.log)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()

	return rf
}

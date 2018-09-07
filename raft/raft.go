package raft

import (
	"encoding/json"
	"fmt"
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Raft struct {
	sync.RWMutex

	me    int
	peers []*labrpc.ClientEnd

	rootInterruptionChs []chan struct{}

	exitRoleFuncs  map[int]exitRoleFunc
	enterRoleFuncs map[int]enterRoleFunc

	majority      int
	votedFor      int
	electionTimer *time.Timer

	currentTerm int
	currentRole int

	log                      []LogEntry
	entriesCh                chan LogEntry
	entriesChBufferSize      int
	matchEntriesCh           chan matchEntry
	matchEntriesChBufferSize int
	commitIndex              int32
	applyEntriesCh           chan struct{}
	applyEntriesChBufferSize int
	appliedEntriesCh         chan ApplyMsg

	shutdownCh chan struct{}

	persister *Persister
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, appliedEntriesCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		appliedEntriesCh: appliedEntriesCh,
	}

	rf.readPersist(persister.ReadRaftState())
	rf.init(len(peers))
	go rf.applyEntries()
	rf.startElectionTimer()

	return rf
}

func (rf *Raft) init(peers int) {
	rf.currentTerm = 0
	rf.votedFor = none
	rf.currentRole = follower
	rf.majority = int(math.Floor(float64(len(rf.peers))/2)) + 1
	rf.log = []LogEntry{LogEntry{Term: 0, Command: nil}}
	// TODO: read from configuration.
	rf.entriesChBufferSize = 100
	// TODO: read from configuration.
	rf.entriesCh = make(chan LogEntry, rf.entriesChBufferSize)
	rf.matchEntriesChBufferSize = 100
	rf.matchEntriesCh = make(chan matchEntry, rf.matchEntriesChBufferSize)
	rf.applyEntriesChBufferSize = 100
	rf.applyEntriesCh = make(chan struct{}, rf.applyEntriesChBufferSize)
	rf.shutdownCh = make(chan struct{})
	rf.setRoleFuncs()
}

func (rf *Raft) setRoleFuncs() {
	rf.exitRoleFuncs = map[int]exitRoleFunc{
		follower:  rf.exitFollower,
		candidate: rf.exitCandidate,
		leader:    rf.exitLeader,
	}

	rf.enterRoleFuncs = map[int]enterRoleFunc{
		follower:  rf.enterFollower,
		candidate: rf.enterCandidate,
		leader:    rf.enterLeader,
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.RLock()
	defer rf.RUnlock()

	if rf.currentRole == leader {
		entry := LogEntry{Term: rf.currentTerm, Command: command}
		rf.log = append(rf.log, entry)
		fmt.Printf("Server %d was given an entry: %+v\n", rf.me, entry)
		rf.entriesCh <- entry
		return len(rf.log) - 1, rf.currentTerm, true
	}

	return 0, rf.currentTerm, false
}

func (rf *Raft) Kill() {

}

func (rf *Raft) GetState() (int, bool) {
	rf.RLock()
	defer rf.RUnlock()
	return rf.currentTerm, rf.currentRole == leader
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	if err := json.Unmarshal(data, rf); err != nil {

	}
}

// Role statemachine.

const (
	follower = iota
	candidate
	leader
	none
)

type state struct {
	currentTerm int
	currentRole int
	votedFor    int
	log         []LogEntry
}

type exitRoleFunc func()

func (rf *Raft) exitFollower() {

}

func (rf *Raft) exitCandidate() {
	rf.votedFor = none
}

func (rf *Raft) exitLeader() {

}

type enterRoleFunc func()

func (rf *Raft) enterFollower() {
	fmt.Printf("Server %d is a follower. Term is %d. Role was %d.\n", rf.me, rf.currentTerm, rf.currentRole)
}

func (rf *Raft) enterCandidate() {
	rf.currentTerm++
	fmt.Printf("Server %d is a candidate. Term is %d. Role was %d.\n", rf.me, rf.currentTerm, rf.currentRole)
	rf.votedFor = rf.me
	lastLogIndex := len(rf.log) - 1
	interruptionCh := make(chan struct{})
	rf.rootInterruptionChs = append(rf.rootInterruptionChs, interruptionCh)
	rf.restartElectionTimer()
	go rf.requestVotes(rf.currentTerm, lastLogIndex, rf.log[lastLogIndex].Term, interruptionCh)
}

func (rf *Raft) enterLeader() {
	rf.stopElectionTimer()
	fmt.Printf("Server %d thinks it is the leader. Term is %d. Role was %d.\n", rf.me, rf.currentTerm, rf.currentRole)
	interruptionCh := make(chan struct{})
	rf.rootInterruptionChs = append(rf.rootInterruptionChs, interruptionCh)
	go rf.commitEntries(rf.currentTerm, interruptionCh)
	go rf.heartbeat(rf.currentTerm, interruptionCh)
	rf.flushEntries()
	log := make([]LogEntry, len(rf.log))
	copy(log, rf.log)
	go rf.replicateLog(rf.currentTerm, log, rf.entriesCh, interruptionCh)
}

// term is the term to set.
func (rf *Raft) transition(role int, term int) {
	if term < rf.currentTerm {
		return
	}

	// TODO: revisit currentRole == role.
	if (rf.currentRole == follower && (role == candidate || role == follower)) ||
		rf.currentRole == candidate || // Candidate can transition to any.
		(rf.currentRole == leader && role == follower) {
		fmt.Printf("Server %d is transitioning from %d to %d.\n", rf.me, rf.currentRole, role)
		rf.interrupt()
		rf.updateTerm(term)
		rf.exitRoleFuncs[rf.currentRole]()
		rf.enterRoleFuncs[role]()
		rf.currentRole = role
	} else {
		panic("invalid transition")
	}
}

func (rf *Raft) lockedTransition(role int, term int) {
	rf.Lock()
	defer rf.Unlock()
	rf.transition(role, term)
}

func (rf *Raft) automaticTransition() {
	rf.Lock()
	defer rf.Unlock()
	fmt.Printf("The timer of server %d expired.\n", rf.me)
	rf.transition(candidate, rf.currentTerm)
}

func (rf *Raft) interrupt() {
	fmt.Println("Interrupting.")
	for _, interruptionCh := range rf.rootInterruptionChs {
		close(interruptionCh)
	}
	rf.rootInterruptionChs = nil
}

func (rf *Raft) updateTerm(term int) {
	if rf.currentTerm < term {
		rf.currentTerm = term
		// Helps to ensure that the follower votes only once per term.
		rf.votedFor = none
	}
}

// Log replication.

type LogEntry struct {
	Term    int         `json:"Term"`
	Command interface{} `json:"Command"`
}

type matchEntry struct {
	index int
	peer  int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Peer    int
}

// log should be a copy, to avoid having to synchronize access to the main log.
func (rf *Raft) replicateLog(term int, log []LogEntry, rootEntriesCh chan LogEntry, rootInterruptionCh chan struct{}) {
	numPeers := len(rf.peers) - 1

	var wg sync.WaitGroup
	var entriesChs []chan LogEntry
	interruptionCh := make(chan struct{})

	defer func() {
		close(interruptionCh)
		wg.Wait()
		for _, entriesCh := range entriesChs {
			close(entriesCh)
		}
	}()

	wg.Add(numPeers)
	for i, peer := range rf.peers {
		if i != rf.me {
			fmt.Printf("Server %d will replicate to peer %d.\n", rf.me, i)
			entriesCh := make(chan LogEntry, rf.entriesChBufferSize)
			entriesChs = append(entriesChs, entriesCh)
			peerLog := make([]LogEntry, len(log))
			// TODO: don't do this.
			copy(peerLog, log)
			go rf.replicateLogToPeer(i, peer, peerLog, entriesCh, term, interruptionCh, &wg)
		}
	}

	for {
		select {
		case entry := <-rootEntriesCh:
			count := 0
			for _, entriesCh := range entriesChs {
				entriesCh <- entry
				count++
			}
			fmt.Printf("Server %d distributed an entry to %d peers.\n", rf.me, count)
		case <-rootInterruptionCh:
			fmt.Println("Root interruption.")
			return
		}
	}
}

func (rf *Raft) replicateLogToPeer(peerNum int, peer *labrpc.ClientEnd, log []LogEntry, entriesCh chan LogEntry, currentTerm int, interruptionCh chan struct{}, wg *sync.WaitGroup) {
	prevLogIndex := len(log) - 1

	args := AppendEntriesArgs{LeaderID: rf.me, Term: currentTerm}
	reply := AppendEntriesReply{}

	defer func() {
		fmt.Printf("Server %d stopped replicating to peer.\n", rf.me)
		wg.Done()
	}()

	fmt.Printf("Server %d is replicating to peer %d.\n", rf.me, peerNum)
	for {
		select {
		case entry := <-entriesCh:
			fmt.Printf("Server %d will replicate entries: %+v\n", rf.me, entry)
			count := 1
			log = append(log, entry)
			// TODO: Blocks. Why?
			// Drain it.
			// for {
			// 	select {
			// 	case entry = <-entriesCh:
			// 		fmt.Printf("Server %d is draining, count=%d, entry=%+v.\n", rf.me, count, entry)
			// 		log = append(log, entry)
			// 		count++
			// 	default:
			// 	}
			// }
			fmt.Printf("Server %d will replicate %d entries.\n", rf.me, count)
		case <-interruptionCh:
			fmt.Printf("Server %d's replication to peer %d interrupted.\n", rf.me, peerNum)
			return
		}

		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = log[prevLogIndex].Term
		args.LeaderCommit = int(atomic.LoadInt32(&rf.commitIndex))
		args.Entries = []LogEntry{}

	loop:
		for {
			select {
			case <-interruptionCh:
				return
			default:
				if prevLogIndex+1 < len(log) {
					args.Entries = log[prevLogIndex+1:]
				}

				// No need for synchronized access until cluster reconfiguration is implemented.
				fmt.Printf("Server %d sending AppendEntries to %d.\n", rf.me, peerNum)
				ok := peer.Call("Raft.AppendEntries", &args, &reply)

				if !ok {
					fmt.Printf("Server %d's AppendEntries to %d failed.\n", rf.me, peerNum)
					break loop
				}

				fmt.Printf("Server %d got AppendEntries reply %+v from %d.\n", rf.me, reply, peerNum)

				if reply.Term > currentTerm {
					rf.lockedTransition(follower, reply.Term)
					return
				}

				if reply.Success {
					prevLogIndex = len(log) - 1
					rf.matchEntriesCh <- matchEntry{index: prevLogIndex, peer: peerNum}
					break loop
				}

				prevLogIndex--
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = log[prevLogIndex].Term
			}
		}
	}
}

// Notes:
//
// a) The RPC's term is equal to or higher than the server's current term. The
//    election timer won't be restarted by out-of-date RPCs.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("Server %d received AppendEntries from %d: %+v\n", rf.me, args.LeaderID, args)
	rf.Lock()
	defer rf.Unlock()
	fmt.Printf("Server %d received AppendEntries from %d and got lock.\n", rf.me, args.LeaderID)

	*reply = AppendEntriesReply{Term: rf.currentTerm, Success: false, Peer: rf.me}

	if args.Term < rf.currentTerm {
		fmt.Printf("AppendEntries request to server %d is stale.\n", rf.me)
		return
	}

	if args.Term > rf.currentTerm {
		rf.transition(follower, args.Term)
	}

	// Note (a).
	rf.restartElectionTimer()

	if len(args.Entries) > 0 {
		// Log consistency check.
		lastLogIndex := len(rf.log) - 1
		if args.PrevLogIndex > lastLogIndex ||
			args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			fmt.Printf("AppendEntries request to server %d failed to pass the log check.\n", rf.me)
			return
		}

		// +1 because the high bound is non-inclusive.
		rf.log = rf.log[0 : args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
	}

	if int(atomic.LoadInt32(&rf.commitIndex)) < args.LeaderCommit {
		atomic.StoreInt32(&rf.commitIndex, int32(min(args.LeaderCommit, len(rf.log)-1)))
		rf.applyEntriesCh <- struct{}{}
	}

	*reply = AppendEntriesReply{Term: args.Term, Success: true, Peer: rf.me}
	fmt.Printf("AppendEntries request to server %d responded: %+v\n", rf.me, *reply)
}

func (rf *Raft) commitEntries(term int, rootInterruptionCh chan struct{}) {
	var majorityIndex int
	matchIndex := make([]int, len(rf.peers))
	commitIndex := int(atomic.LoadInt32(&rf.commitIndex))

	fmt.Printf("Server %d is committing entries.\n", rf.me)

	// TODO: document invariants.
	for {
		select {
		case entry := <-rf.matchEntriesCh:
			fmt.Printf("Server %d received matching entry %+v.\n", rf.me, entry)
			matchIndex[entry.peer] = entry.index

			for {
				matches := 1
				for peer := range rf.peers {
					if peer != rf.me {
						if matchIndex[peer] > majorityIndex {
							matches++
						}
					}
				}
				if matches < rf.majority {
					break
				}
				majorityIndex++
			}

			if majorityIndex > commitIndex {
				rf.RLock()
				if rf.log[majorityIndex].Term == term {
					commitIndex = majorityIndex
					atomic.StoreInt32(&rf.commitIndex, int32(commitIndex))
					rf.applyEntriesCh <- struct{}{}
					fmt.Printf("Server %d committed entries up to index %d.\n", rf.me, commitIndex)
				}
				rf.RUnlock()
			}
		case <-rootInterruptionCh:
			return
		}
	}
}

func (rf *Raft) applyEntries() {
	fmt.Printf("Server %d is applying entries.\n", rf.me)

	var lastAppliedIndex int
	for {
		select {
		case <-rf.applyEntriesCh:
			commitIndex := int(atomic.LoadInt32(&rf.commitIndex))
			rf.RLock()
			for applyIndex := lastAppliedIndex + 1; applyIndex <= commitIndex; applyIndex++ {
				rf.appliedEntriesCh <- ApplyMsg{Index: applyIndex, Command: rf.log[applyIndex].Command}
				fmt.Printf("Server %d applied entry (%d, %d, %+v).\n", rf.me, applyIndex, rf.log[applyIndex].Term, rf.log[applyIndex].Command)
			}
			lastAppliedIndex = commitIndex
			rf.RUnlock()
		case <-rf.shutdownCh:
			return
		}
	}
}

func (rf *Raft) flushEntries() {
	for {
		select {
		case <-rf.entriesCh:
		default:
			return
		}
	}
}

// Heartbeating.

func (rf *Raft) heartbeat(term int, rootInterruptionCh chan struct{}) {
	heartbeat := time.NewTicker(ETO / 2 * time.Millisecond)
	defer func() {
		fmt.Printf("Server %d stopped heartbeating.\n", rf.me)
		heartbeat.Stop()
	}()

	for {
		select {
		case <-heartbeat.C:
			for i, peer := range rf.peers {
				if i != rf.me {
					go rf.heartbeatToPeer(i, peer, term)
				}
			}
		case <-rootInterruptionCh:
			return
		}
	}
}

func (rf *Raft) heartbeatToPeer(peerNum int, peer *labrpc.ClientEnd, term int) {
	args := AppendEntriesArgs{LeaderID: rf.me, Term: term, Entries: []LogEntry{}, LeaderCommit: int(atomic.LoadInt32(&rf.commitIndex))}
	reply := AppendEntriesReply{}

	ok := peer.Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		fmt.Printf("Server %d's heartbeat to %d failed.\n", rf.me, peerNum)
		return
	}

	if reply.Term > term {
		rf.lockedTransition(follower, reply.Term)
		return
	}
}

// Leader election.

const (
	// TODO: put these in a persistent config object.
	ETOBase   = 150
	ETOLength = 150
	ETO       = ETOBase
)

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// The log can't change while the candidate is requesting votes legitimately.
func (rf *Raft) requestVotes(electionTerm, lastLogIndex, lastLogTerm int, rootInterruptionCh chan struct{}) {
	numPeers := len(rf.peers) - 1

	var wg sync.WaitGroup
	repliesCh := make(chan RequestVoteReply, numPeers)
	interruptionCh := make(chan struct{})

	defer func() {
		close(interruptionCh)
		wg.Wait()
		close(repliesCh)
	}()

	wg.Add(numPeers)
	for i, peer := range rf.peers {
		if i != rf.me {
			go rf.requestVote(i, peer, electionTerm, lastLogIndex, lastLogTerm, repliesCh, interruptionCh, &wg)
		}
	}

	votes := 1
	var done int
	for done < numPeers {
		select {
		case reply := <-repliesCh:
			if reply.Term > electionTerm {
				go rf.lockedTransition(follower, reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= rf.majority {
					go rf.lockedTransition(leader, electionTerm)
					return
				}
			}
			done++
		case <-rootInterruptionCh:
			return
		}
	}
}

func (rf *Raft) requestVote(peerNum int, peer *labrpc.ClientEnd, electionTerm, lastLogIndex, lastLogTerm int, replyCh chan RequestVoteReply, interruptionCh chan struct{}, wg *sync.WaitGroup) {
	fmt.Printf("Server %d is requesting the vote of peer %d.\n", rf.me, peerNum)

	args := RequestVoteArgs{
		Term:         electionTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// In case of interruption, harmless values.
	reply := RequestVoteReply{Term: electionTerm}
	defer func() {
		replyCh <- reply
		wg.Done()
	}()

	var ok bool
	for !ok {
		select {
		case <-interruptionCh:
			return
		default:
			fmt.Printf("Server %d sending RequestVote to %d.\n", rf.me, peerNum)
			ok = peer.Call("Raft.Vote", &args, &reply)
			if ok {
				fmt.Printf("Server %d (%d) got RequestVote reply %+v from %d.\n", rf.me, rf.currentTerm, reply, peerNum)
			} else {
				fmt.Printf("Server %d's RequestVote to %d failed.\n", rf.me, peerNum)
			}
		}
	}
}

// Notes:
//
// a) It may have voted in this term already because: a) it is also a
//    candidate running for the same term, b) a previous request caused
//    it to transition to follower, but it did not grant it its vote;
//    instead, a competing candidate requested its vote shortly after.
//
// b) A candidate whose log is not complete is allowed to cause a leader
//    to step down (the leader would update its term, step down, and then
//    deny its vote): an isolated candidate that's been increasing its
//    term timeout after timeout for a while and that rejoins the network
//    is allowed to disrupt the progress of the replication taking place
//    in the term (older) where the majority is. Should the leader ignore
//    the higher term of the candidate, the isolated candidate would not
//    be able to participate anymore, because it would be rejecting every
//    AppendEntriesRPC that it receives, considering the RPC's term stale.
//
// c) If the terms were the same, the voter couldn't have voted already.
//    If the term was higher, the transition should have cleared the vote.
func (rf *Raft) Vote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("Server %d received RequestVote from %d: %+v\n", rf.me, args.CandidateID, args)
	rf.Lock()
	defer rf.Unlock()
	fmt.Printf("Server %d received RequestVote from %d and got lock.\n", rf.me, args.CandidateID)

	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}

	if args.Term < rf.currentTerm ||
		// Has voted in this term already. Note (a).
		(args.Term == rf.currentTerm && rf.votedFor != none) {
		// DEBUG.
		if args.Term == rf.currentTerm && rf.votedFor != none {
			fmt.Printf("Server %d already voted for %d in term %d.\n", rf.me, rf.votedFor, rf.currentTerm)
		} else {
			fmt.Printf("Server %d denied vote to %d because of stale term %d.\n", rf.me, args.CandidateID, args.Term)
		}
		return
	}

	// Note (b).
	fmt.Printf("Server %d is transitioning/voting.\n", rf.me)
	rf.transition(follower, args.Term)

	// Note (c).
	if rf.votedFor != none {
		panic("invalid vote")
	}

	// Candidate completeness check.
	lastLogIndex := len(rf.log) - 1
	if args.LastLogTerm < rf.log[lastLogIndex].Term ||
		(args.LastLogTerm == rf.log[lastLogIndex].Term &&
			args.LastLogIndex < lastLogIndex) {
		fmt.Printf("Vote request to server %d failed to pass the candidate completeness check.\n", rf.me)
		return
	}

	rf.restartElectionTimer()

	rf.votedFor = args.CandidateID
	*reply = RequestVoteReply{Term: args.Term, VoteGranted: true}
	fmt.Printf("Vote request to server %d responded: %+v\n", rf.me, *reply)
}

// Timer.

func (rf *Raft) startElectionTimer() {
	to := time.Duration(ETOBase + rand.Intn(ETOLength))
	rf.electionTimer = time.AfterFunc(to*time.Millisecond, rf.automaticTransition)
	fmt.Printf("Server %d started the timer.\n", rf.me)
}

func (rf *Raft) restartElectionTimer() {
	to := time.Duration(ETOBase + rand.Intn(ETOLength))
	rf.electionTimer.Reset(to * time.Millisecond)
	fmt.Printf("Server %d restarted the timer.\n", rf.me)
}

func (rf *Raft) stopElectionTimer() {
	rf.electionTimer.Stop()
	fmt.Printf("Server %d stopped the timer.\n", rf.me)
}

// Util.

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

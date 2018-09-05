package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestVote(t *testing.T) {
	type testCase struct {
		name          string
		server        Raft
		args          RequestVoteArgs
		expectedReply RequestVoteReply
		expectedState Raft
	}

	testCases := []testCase{
		{
			"1st election, 1st request, granted",
			Raft{
				currentTerm: 1,
				currentRole: follower,
				votedFor:    none,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			RequestVoteArgs{
				Term:         1,
				CandidateID:  1,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			RequestVoteReply{
				Term:        1,
				VoteGranted: true,
			},
			Raft{
				currentTerm: 1,
				currentRole: follower,
				votedFor:    1,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
		{
			"1st election, voted already, denied",
			Raft{
				currentTerm: 1,
				currentRole: follower,
				votedFor:    2,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			RequestVoteArgs{
				Term:         1,
				CandidateID:  1,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			RequestVoteReply{
				Term:        1,
				VoteGranted: false,
			},
			Raft{
				currentTerm: 1,
				currentRole: follower,
				votedFor:    2,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
		{
			"Stale candidate request, denied",
			Raft{
				currentTerm: 2,
				currentRole: follower,
				votedFor:    none,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			RequestVoteArgs{
				Term:         1,
				CandidateID:  1,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			RequestVoteReply{
				Term:        2,
				VoteGranted: false,
			},
			Raft{
				currentTerm: 2,
				currentRole: follower,
				votedFor:    none,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
		{
			"Stale candidate voter, drop out of race, granted",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: candidate,
				votedFor:    2,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			RequestVoteArgs{
				Term:         2,
				CandidateID:  1,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			RequestVoteReply{
				Term:        2,
				VoteGranted: true,
			},
			Raft{
				currentTerm: 2,
				currentRole: follower,
				votedFor:    1,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
		{
			"Stale leader voter, step down, granted",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: leader,
				votedFor:    none,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			RequestVoteArgs{
				Term:         2,
				CandidateID:  1,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			RequestVoteReply{
				Term:        2,
				VoteGranted: true,
			},
			Raft{
				currentTerm: 2,
				currentRole: follower,
				votedFor:    1,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
	}

	for i := range testCases {
		tc := &testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			tc.server.setRoleFuncs()
			reply := RequestVoteReply{}
			tc.server.Vote(&tc.args, &reply)
			if reply != tc.expectedReply {
				t.Errorf("reply: got %+v, want %+v", reply, tc.expectedReply)
			}
			if tc.server.currentTerm != tc.expectedState.currentTerm {
				t.Errorf("currentTerm: got %+v, want %+v", tc.server.currentTerm, tc.expectedState.currentTerm)
			}
			if tc.server.currentRole != tc.expectedState.currentRole {
				t.Errorf("currentRole: got %+v, want %+v", tc.server.currentRole, tc.expectedState.currentRole)
			}
			if tc.server.votedFor != tc.expectedState.votedFor {
				t.Errorf("votedFor: got %+v, want %+v", tc.server.votedFor, tc.expectedState.votedFor)
			}
		})
	}
}

func TestAppendEntries(t *testing.T) {
	type testCase struct {
		name          string
		server        Raft
		args          AppendEntriesArgs
		expectedReply AppendEntriesReply
		expectedState Raft
	}

	testCases := []testCase{
		{
			"1st heartbeat, success",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesArgs{
				Term:         1,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesReply{
				Term:    1,
				Success: true,
			},
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
		{
			"Heartbeat, leader, transition, success",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: leader,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesArgs{
				Term:         2, // Newer term.
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesReply{
				Term:    2,
				Success: true,
			},
			Raft{
				me:          2,
				currentTerm: 2,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
		{
			"Heartbeat, candidate, transition, success",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: candidate,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesArgs{
				Term:         2, // Newer term.
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesReply{
				Term:    2,
				Success: true,
			},
			Raft{
				me:          2,
				currentTerm: 2,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
		{
			"1st entry, success",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesArgs{
				Term:         1,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{LogEntry{Term: 1, Command: "cmd"}},
			},
			AppendEntriesReply{
				Term:    1,
				Success: true,
			},
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}, LogEntry{Term: 1, Command: "cmd"}},
			},
		},
		{
			"2nd entry, success",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}, LogEntry{Term: 1, Command: "cmd"}},
			},
			AppendEntriesArgs{
				Term:         1,
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries:      []LogEntry{LogEntry{Term: 1, Command: "cmd"}},
			},
			AppendEntriesReply{
				Term:    1,
				Success: true,
			},
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}, LogEntry{Term: 1, Command: "cmd"}, LogEntry{Term: 1, Command: "cmd"}},
			},
		},
		{
			"Multiple entries, success",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesArgs{
				Term:         1,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{LogEntry{Term: 1, Command: "cmd"}, LogEntry{Term: 1, Command: "cmd"}},
			},
			AppendEntriesReply{
				Term:    1,
				Success: true,
			},
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}, LogEntry{Term: 1, Command: "cmd"}, LogEntry{Term: 1, Command: "cmd"}},
			},
		},
		{
			"Stale leader, failure",
			Raft{
				me:          2,
				currentTerm: 2,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesArgs{
				Term:         1, // Old term.
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{},
			},
			AppendEntriesReply{
				Term:    2,
				Success: false,
			},
			Raft{
				me:          2,
				currentTerm: 2,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
		{
			"Request doesn't match log, failure",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesArgs{
				Term:         1,
				PrevLogIndex: 1, // Follower doesn't have previous entry.
				PrevLogTerm:  1,
				Entries:      []LogEntry{LogEntry{Term: 1, Command: "cmd"}},
			},
			AppendEntriesReply{
				Term:    1,
				Success: false,
			},
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
		},
		{
			"Follower has extra entries, remove, success",
			Raft{
				me:          2,
				currentTerm: 1,
				currentRole: follower,
				// Uncommitted entry from term 1. Current term is 2.
				log: []LogEntry{LogEntry{Term: 0, Command: nil}, LogEntry{Term: 1, Command: "cmd"}},
			},
			AppendEntriesArgs{
				Term:         2,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{LogEntry{Term: 0, Command: nil}},
			},
			AppendEntriesReply{
				Term:    2,
				Success: true,
			},
			Raft{
				me:          2,
				currentTerm: 2, // Updated term.
				currentRole: follower,
				log:         []LogEntry{LogEntry{Term: 0, Command: nil}}, // Removed entry.
			},
		},
	}

	for i := range testCases {
		tc := &testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			tc.server.setRoleFuncs()
			reply := AppendEntriesReply{}
			tc.server.AppendEntries(&tc.args, &reply)
			if reply != tc.expectedReply {
				t.Errorf("reply: got %+v, want %+v", reply, tc.expectedReply)
			}
			if tc.server.currentTerm != tc.expectedState.currentTerm {
				t.Errorf("currentTerm: got %+v, want %+v", tc.server.currentTerm, tc.expectedState.currentTerm)
			}
			if tc.server.currentRole != tc.expectedState.currentRole {
				t.Errorf("currentRole: got %+v, want %+v", tc.server.currentRole, tc.expectedState.currentRole)
			}
			for i := range tc.expectedState.log {
				if i >= len(tc.server.log) {
					t.Errorf("log: index %d exceeds length", i)
					break
				}
				if tc.server.log[i] != tc.expectedState.log[i] {
					t.Errorf("log: got %+v, want %+v at index %d", tc.server.log[i], tc.expectedState.log[i], i)
					break
				}
			}
		})
	}
}

func TestReplicateLog(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	cfg.checkOneLeader()
	fmt.Println("One-leader check.")
	time.Sleep(10 * time.Second)

	// var ldr *Raft
	var leaders int
	for _, svr := range cfg.rafts {
		fmt.Printf("Server %d  has %d peers.\n", svr.me, len(svr.peers))
		if svr.currentRole == leader {
			// ldr = svr
			// break
			leaders++
		}
	}
	fmt.Printf("There are %d leaders.\n", leaders)
}

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	rand2 "math/rand"
	"sort"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// add 随机超时选举，[electionTimeout, 2*electionTimeout)[150ms,300ms]
	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, conState, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = conState.Nodes
	}
	rf := &Raft{
		id:               c.ID,
		Term:             hardState.Term,    // Term 和 Vote 从持久化存储中读取
		Vote:             hardState.Vote,    // Term 和 Vote 从持久化存储中读取
		RaftLog:          newLog(c.Storage), // Log 也从持久化存储中读取
		Prs:              map[uint64]*Progress{},
		State:            StateFollower,
		votes:            map[uint64]bool{},
		msgs:             nil,
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	//生成随机选举超时时间
	rf.resetRandomizedElectionTimeout()

	// 更新集群配置
	rf.Prs = make(map[uint64]*Progress)
	for _, id := range c.peers {
		rf.Prs[id] = &Progress{}
	}

	return rf
}

// resetRandomizedElectionTimeout 生成随机选举超时时间，范围在 [r.electionTimeout, 2*r.electionTimeout]
func (r *Raft) resetRandomizedElectionTimeout() {
	rand2.Seed(time.Now().UnixNano())
	//[0,n)+electionTimeout
	//[electionTimeout,electionTimeout*2)
	r.randomElectionTimeout = r.electionTimeout + rand2.Intn(r.electionTimeout)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// prevLogIndex 和 prevLogTerm 用来判断 leader 和 follower 的日志是否冲突
	// 如果没有冲突的话就会将 Entries 追加或者覆盖到 follower 的日志中
	// nextIndex存储的是下一次给该节点同步日志时的日志索引。
	// matchIndex存储的是该节点的最大日志索引。
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err == nil {
		// 没有错误发生，说明 prevLogIndex 的下一条日志（nextIndex）位于内存日志数组中
		// 此时可以发送 Entries 数组给 followers
		appendMsg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      to,
			Term:    r.Term,
			LogTerm: prevLogTerm,
			Index:   prevLogIndex,
			Entries: make([]*pb.Entry, 0),
			Commit:  r.RaftLog.committed,
		}
		// 期望覆盖或者追加到 follower 上的日志集合
		nextEntries := r.RaftLog.getEntries(prevLogIndex+1, 0)
		for i := range nextEntries {
			appendMsg.Entries = append(appendMsg.Entries, &nextEntries[i])
		}
		r.msgs = append(r.msgs, appendMsg)
		return true
	}
	// 有错误，说明 nextIndex 存在于快照中，此时需要发送快照给 followers
	//2C
	r.sendSnapshot(to)
	log.Infof("[Snapshot Request]%d to %d, prevLogIndex %v, dummyIndex %v", r.id, to, prevLogIndex, r.RaftLog.dummyIndex)

	return false
}

// sendSnapshot 发送快照给别的节点
func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		// 生成 Snapshot 的工作是由 region worker 异步执行的，如果 Snapshot 还没有准备好
		// 此时会返回 ErrSnapshotTemporarilyUnavailable 错误，此时 leader 应该放弃本次 Snapshot Request
		// 等待下一次再请求 storage 获取 snapshot（通常来说会在下一次 heartbeat response 的时候发送 snapshot）
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.leaderTick()
	case StateCandidate:
		r.candidateTick()
	case StateFollower:
		r.followerTick()
	}
}

func (r *Raft) leaderTick() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		// MessageType_MsgBeat 属于内部消息，不需要经过 RawNode 处理
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})
	}
	//TODO 选举超时 判断心跳回应数量

	//TODO 3A 禅让机制
}
func (r *Raft) candidateTick() {
	r.electionElapsed++
	// 选举超时 发起选举
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		// MessageType_MsgHup 属于内部消息，也不需要经过 RawNode 处理
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
	}
}
func (r *Raft) followerTick() {
	r.electionElapsed++
	// 选举超时 发起选举
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		// MessageType_MsgHup 属于内部消息，也不需要经过 RawNode 处理
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		// 只有 Term > currentTerm 的时候才需要对 Vote 进行重置
		// 这样可以保证在一个任期内只会进行一次投票
		r.Vote = None
	}
	r.Term = term
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
	r.leadTransferee = None
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate // 0. 更改自己的状态
	r.Term++                 // 1. 增加自己的任期
	r.Vote = r.id            // 2. 投票给自己
	r.votes[r.id] = true

	r.electionElapsed = 0 // 3. 重置超时选举计时器
	r.resetRandomizedElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	//初始化 nextIndex 和 matchIndex
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1 // 初始化为 leader 的最后一条日志索引（后续出现冲突会往前移动）
		r.Prs[id].Match = 0                        // 初始化为 0 就可以了
	}

	// 成为 Leader 之后立马在日志中追加一条 noop 日志，这是因为
	// 在 Raft 论文中提交 Leader 永远不会通过计算副本的方式提交一个之前任期、并且已经被复制到大多数节点的日志
	// 通过追加一条当前任期的 noop 日志，可以快速的提交之前任期内所有被复制到大多数节点的日志
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		//Follower 可以接收到的消息：
		//MsgHup、MsgRequestVote、MsgHeartBeat、MsgAppendEntry
		r.followerStep(m)
	case StateCandidate:
		//Candidate 可以接收到的消息：
		//MsgHup、MsgRequestVote、MsgRequestVoteResponse、MsgHeartBeat
		r.candidateStep(m)
	case StateLeader:
		//Leader 可以接收到的消息：
		//MsgBeat、MsgHeartBeatResponse、MsgRequestVote
		r.leaderStep(m)
	}
	return nil
}

func (r *Raft) followerStep(m pb.Message) {
	//Follower 可以接收到的消息：
	//MsgHup、MsgRequestVote、MsgHeartBeat、MsgAppendEntry
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		//Local Msg，用于请求节点开始选举，仅仅需要一个字段。
		//TODO MsgHup
		// 成为候选者，开始发起投票
		r.handleStartElection(m)
	case pb.MessageType_MsgBeat:
		//Local Msg，用于告知 Leader 该发送心跳了，仅仅需要一个字段。
		//TODO Follower No processing required
	case pb.MessageType_MsgPropose:
		//Local Msg，用于上层请求 propose 条目
		//TODO Follower No processing required
	case pb.MessageType_MsgAppend:
		//Common Msg，用于 Leader 给其他节点同步日志条目
		//TODO MsgAppendEntry
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		//Common Msg，用于节点告诉 Leader 日志同步是否成功，和 MsgAppend 对应
		//TODO Follower No processing required
	case pb.MessageType_MsgRequestVote:
		//Common Msg，用于 Candidate 请求投票
		// TODO MsgRequestVote
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		//Common Msg，用于节点告诉 Candidate 投票结果
		//TODO Follower No processing required
	case pb.MessageType_MsgSnapshot:
		//Common Msg，用于 Leader 将快照发送给其他节点
		//TODO Follower No processing required
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		//Common Msg，即 Leader 发送的心跳。
		//不同于论文中使用空的追加日志 RPC 代表心跳，TinyKV 给心跳一个单独的 MsgType
		//TODO MsgHeartbeat
		// 接收心跳包，重置超时，称为跟随者，回发心跳包的resp
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		//Common Msg，即节点对心跳的回应
		//TODO Follower No processing required
	case pb.MessageType_MsgTransferLeader:
		//Local Msg，用于上层请求转移 Leader
		//TODO Follower No processing required
	case pb.MessageType_MsgTimeoutNow:
		//Local Msg，节点收到后清空 r.electionElapsed，并即刻发起选举
		//TODO Follower No processing required
	}
}

func (r *Raft) candidateStep(m pb.Message) {
	//Candidate 可以接收到的消息：
	//MsgHup、MsgRequestVote、MsgRequestVoteResponse、MsgHeartBeat
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		//Local Msg，用于请求节点开始选举，仅仅需要一个字段。
		//TODO MsgHup
		// 成为候选者，开始发起投票
		r.handleStartElection(m)
	case pb.MessageType_MsgBeat:
		//Local Msg，用于告知 Leader 该发送心跳了，仅仅需要一个字段。
		//TODO Candidate No processing required
	case pb.MessageType_MsgPropose:
		//Local Msg，用于上层请求 propose 条目
		//TODO Candidate No processing required
	case pb.MessageType_MsgAppend:
		//Common Msg，用于 Leader 给其他节点同步日志条目
		//TODO MsgAppendEntry
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		//Common Msg，用于节点告诉 Leader 日志同步是否成功，和 MsgAppend 对应
		//TODO Candidate No processing required
	case pb.MessageType_MsgRequestVote:
		//Common Msg，用于 Candidate 请求投票
		// TODO MsgRequestVote
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		//Common Msg，用于节点告诉 Candidate 投票结果
		// TODO MsgRequestVoteResponse
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		//Common Msg，用于 Leader 将快照发送给其他节点
		//TODO Candidate No processing required
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		//Common Msg，即 Leader 发送的心跳。
		//不同于论文中使用空的追加日志 RPC 代表心跳，TinyKV 给心跳一个单独的 MsgType
		//TODO MsgHeartbeat
		// 接收心跳包，重置超时，称为跟随者，回发心跳包的resp
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		//Common Msg，即节点对心跳的回应
		//TODO Candidate No processing required
	case pb.MessageType_MsgTransferLeader:
		//Local Msg，用于上层请求转移 Leader
		//要求领导转移其领导权
		//TODO Candidate No processing required
	case pb.MessageType_MsgTimeoutNow:
		//Local Msg，节点收到后清空 r.electionElapsed，并即刻发起选举
		//从领导发送到领导转移目标，以让传输目标立即超时并开始新的选择。
		//TODO Candidate No processing required
	}
}
func (r *Raft) leaderStep(m pb.Message) {
	//Leader 可以接收到的消息：
	//MsgBeat、MsgHeartBeatResponse、MsgRequestVote、MsgPropose、MsgAppendResponse、MsgAppend
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		//Local Msg，用于请求节点开始选举，仅仅需要一个字段。
		//TODO Leader No processing required
	case pb.MessageType_MsgBeat:
		//Local Msg，用于告知 Leader 该发送心跳了，仅仅需要一个字段。
		//TODO MsgBeat
		r.broadcastHeartBeat()
	case pb.MessageType_MsgPropose:
		//Local Msg，用于上层请求 propose 条目
		//TODO MsgPropose
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		//Common Msg，用于 Leader 给其他节点同步日志条目
		//TODO 网络分区的情况，也是要的
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		//Common Msg，用于节点告诉 Leader 日志同步是否成功，和 MsgAppend 对应
		//TODO MsgAppendResponse
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		//Common Msg，用于 Candidate 请求投票
		// TODO MsgRequestVote
		r.handleRequestVote(m)

	case pb.MessageType_MsgRequestVoteResponse:
		//Common Msg，用于节点告诉 Candidate 投票结果
		//TODO Leader No processing required
	case pb.MessageType_MsgSnapshot:
		//Common Msg，用于 Leader 将快照发送给其他节点
		//3A
		//TODO project2C

	case pb.MessageType_MsgHeartbeat:
		//Common Msg，即 Leader 发送的心跳。
		//不同于论文中使用空的追加日志 RPC 代表心跳，TinyKV 给心跳一个单独的 MsgType
		//TODO Leader No processing required
	case pb.MessageType_MsgHeartbeatResponse:
		//Common Msg，即节点对心跳的回应
		//TODO MsgHeartBeatResponse
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		//Local Msg，用于上层请求转移 Leader
		//要求领导转移其领导权
		//TODO project3
	case pb.MessageType_MsgTimeoutNow:
		//Local Msg，节点收到后清空 r.electionElapsed，并即刻发起选举
		//从领导发送到领导转移目标，以让传输目标立即超时并开始新的选择。
		//TODO project3
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

/* ******************************** Msg Handle ******************************** */

// 成为候选者，开始发起投票
func (r *Raft) handleStartElection(m pb.Message) {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	//发起 RequestVote 请求，消息中的数据按照 Figure4 中进行填充。设置所有需要发送的消息
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote, // 消息类型,发起投票
			To:      id,                            // 节点 ID
			From:    r.id,                          // 候选者编号
			Term:    r.Term,                        // 候选者任期
			LogTerm: r.RaftLog.LastTerm(),          // 候选者最后一条日志的索引
			Index:   r.RaftLog.LastIndex(),         // 候选者最后一条日志的任期
		})
	}
	r.votes = make(map[uint64]bool) // 重置 votes
	r.votes[r.id] = true            // 开始的时候只有自己投票给自己
}

// handleRequestVote 节点收到 RequestVote 请求时候的处理
func (r *Raft) handleRequestVote(m pb.Message) {
	voteRep := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	//1. 判断 Msg 的 Term 是否大于等于自己的 Term，是则更新
	if m.Term > r.Term {
		// term比自己大 变成follower
		r.becomeFollower(m.Term, None)
	}
	if (m.Term > r.Term || m.Term == r.Term && (r.Vote == None || r.Vote == m.From)) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
		// 投票
		// 1. Candidate 任期大于自己并且日志足够新
		// 2. Candidate 任期和自己相等并且自己在当前任期内没有投过票或者已经投给了 Candidate，并且 Candidate 的日志足够新
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
	} else {
		// 拒绝投票
		// 1. Candidate 的任期小于自己
		// 2. 自己在当前任期已经投过票了
		// 3. Candidate 的日志不够新
		voteRep.Reject = true
	}
	r.msgs = append(r.msgs, voteRep)
}

// handleAppendEntries handle AppendEntries RPC request
// 用于 Leader 给其他节点同步日志条目
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	// if term < currentTerm  reply false
	// 1. m.term < r.term false

	// 不包含  prevLogIndex    reply false
	// 2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm

	// 如果现有条目与新条目冲突，则删除现有条目，然后全部跟随该条目
	// 3. if an existing entry conflict with a new one, delete the existing entry and all follow it

	// 附加日志中尚未添加的任何新条目
	// 4. append any new entries not already in the log

	// 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	appendEntryResp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	appendEntryResp.Reject = true

	//1.
	if m.Term < r.Term {
		r.msgs = append(r.msgs, appendEntryResp)
		return
	}
	//2.
	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm
	r.becomeFollower(m.Term, m.From)
	if prevLogIndex > r.RaftLog.LastIndex() || r.RaftLog.TermNoErr(prevLogIndex) != prevLogTerm {
		//最后一条index和preLogIndex冲突、或者任期冲突
		//这时不能直接将 leader 传递过来的 Entries 覆盖到 follower 日志上
		//这里可以直接返回，以便让 leader 尝试 prevLogIndex - 1 这条日志
		//但是这样 follower 和 leader 之间的同步比较慢
		//TODO 日志冲突优化：
		//	找到冲突任期的第一条日志，下次 leader 发送 AppendEntry 的时候会将 nextIndex 设置为 ConflictIndex
		// 	如果找不到的话就设置为 prevLogIndex 的前一个
		appendEntryResp.Index = r.RaftLog.LastIndex()
		//appendEntryResp.Index = prevLogIndex - 1 // 用于提示 leader prevLogIndex 的开始位置是appendEntryResp.Index
		if prevLogIndex <= r.RaftLog.LastIndex() {
			conflictTerm := r.RaftLog.TermNoErr(prevLogIndex)
			for _, ent := range r.RaftLog.entries {
				if ent.Term == conflictTerm {
					//找到冲突任期的上一个任期的idx位置
					appendEntryResp.Index = ent.Index - 1
					break
				}
			}
		}
	} else {
		//prevLogIndex没有冲突
		if len(m.Entries) > 0 {
			//3.
			idx, newLogIndex := m.Index+1, m.Index+1
			// 找到 follower 和 leader 在 new log 中出现冲突的位置
			// 这里是在上面的if break之后再次发来的同步
			for ; idx < r.RaftLog.LastIndex() && idx <= m.Entries[len(m.Entries)-1].Index; idx++ {
				term, _ := r.RaftLog.Term(idx)
				if term != m.Entries[idx-newLogIndex].Term {
					break
				}
			}
			//被break处理了，发现冲突append logs和已有日志冲突
			if idx-newLogIndex != uint64(len(m.Entries)) {
				r.RaftLog.truncate(idx)                               // 截断冲突后面的所有日志
				r.RaftLog.appendNewEntry(m.Entries[idx-newLogIndex:]) // 并追加新的的日志
				r.RaftLog.stabled = min(r.RaftLog.stabled, idx-1)     // 更新持久化的日志索引
			}
		}
		// 更新 commitIndex
		if m.Commit > r.RaftLog.committed {
			// 取当前节点「已经和 leader 同步的日志」和 leader 「已经提交日志」索引的最小值作为节点的 commitIndex
			r.RaftLog.commit(min(m.Commit, m.Index+uint64(len(m.Entries))))
		}
		//同意
		appendEntryResp.Reject = false
		// 用于 leader 更新 NextIndex（存储的是下一次 AppendEntry 的 prevIndex）
		appendEntryResp.Index = m.Index + uint64(len(m.Entries))
		// 用于 leader 更新 committed
		appendEntryResp.LogTerm = r.RaftLog.TermNoErr(appendEntryResp.Index)
	}
	//回发
	r.msgs = append(r.msgs, appendEntryResp)
}
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	//被拒绝
	if m.Reject {
		if m.Term > r.Term {
			// 任期大于自己，那么就变为 Follower
			r.becomeFollower(m.Term, None)
		} else {
			// 否则就是因为 prevLog 日志冲突，继续尝试对 follower 同步日志
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From) // 再次发送 AppendEntry Request
		}
		return
	}
	if r.Prs[m.From].maybeUpdate(m.Index) {
		// 由于有新的日志被复制了，因此有可能有新的日志可以提交执行，所以判断一下
		if r.maybeCommit() {
			// 广播更新所有 follower 的 commitIndex
			r.broadcastAppendEntry()
		}
	}
}

// maybeUpdate 检查日志同步是不是一个过期的回复
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	// 判断是否是过期的消息回复
	if pr.Match < n {
		pr.Match = n
		pr.Next = pr.Match + 1
		updated = true
	}
	return updated
}

// maybeCommit 判断是否有新的日志需要提交
func (r *Raft) maybeCommit() bool {
	matchArray := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchArray = append(matchArray, progress.Match)
	}
	// 获取所有节点 match 的中位数，就是被大多数节点复制的日志索引
	sort.Sort(sort.Reverse(matchArray))
	majority := len(r.Prs)/2 + 1
	toCommitIndex := matchArray[majority-1]
	// 检查是否可以提交 toCommitIndex
	return r.RaftLog.maybeCommit(toCommitIndex, r.Term)
}
func (r *Raft) broadcastAppendEntry() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// handleHeartbeat handle Heartbeat RPC request
// 接收心跳包，重置超时，称为跟随者，回发心跳包的resp
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	heartBeatResp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	if r.Term > m.Term {
		heartBeatResp.Reject = true //拒绝接收
	} else {
		r.becomeFollower(m.Term, m.From)
	}
	r.msgs = append(r.msgs, heartBeatResp)
}

// 从 SnapshotMetadata 中恢复 Raft 的内部状态，例如 term、commit、membership information
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		Term:    r.Term,
	}
	meta := m.Snapshot.Metadata

	// 1. 如果 term 小于自身的 term 直接拒绝这次快照的数据
	if m.Term < r.Term {
		resp.Reject = true
	} else if r.RaftLog.committed >= meta.Index {
		// 2. 如果已经提交的日志大于等于快照中的日志，也需要拒绝这次快照
		// 因为 commit 的日志必定会被 apply，如果被快照中的日志覆盖的话就会破坏一致性
		resp.Reject = true
		resp.Index = r.RaftLog.committed
	} else {
		// 3. 需要安装日志
		r.becomeFollower(m.Term, m.From)
		// 更新日志数据
		r.RaftLog.dummyIndex = meta.Index + 1
		r.RaftLog.committed = meta.Index
		r.RaftLog.applied = meta.Index
		r.RaftLog.stabled = meta.Index
		r.RaftLog.pendingSnapshot = m.Snapshot
		r.RaftLog.entries = make([]pb.Entry, 0)
		// 更新集群配置
		r.Prs = make(map[uint64]*Progress)
		for _, id := range meta.ConfState.Nodes {
			r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		}
		// 更新 response，提示 leader 更新 nextIndex
		resp.Index = meta.Index
	}
	r.msgs = append(r.msgs, resp)
}

// handleRequestVoteResponse 节点收到 RequestVote Response 时候的处理
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	//记录投票
	r.votes[m.From] = !m.Reject
	// 更新节点的投票信息
	count := 0
	for _, agree := range r.votes {
		if agree {
			count++
		}
	}
	majority := len(r.Prs)/2 + 1
	//拒绝
	if m.Reject {
		//自己比别人的trem小
		if r.Term < m.Term {
			// 如果某个节点拒绝了 RequestVote 请求，并且任期大于自身的任期
			// 那么 Candidate 回到 Follower 状态
			r.becomeFollower(m.Term, None)
		}
		if len(r.votes)-count >= majority {
			// 半数以上节点拒绝投票给自己，此时需要变回 follower
			r.becomeFollower(r.Term, None)
		}
	} else {
		//看看是否是大多数
		if count >= majority {
			// 半数以上节点投票给自己，此时可以成为 leader
			r.becomeLeader()
		}
	}
}

// broadcastHeartBeat 广播心跳消息
func (r *Raft) broadcastHeartBeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
	r.heartbeatElapsed = 0
	// 等待 RawNode 取走 r.msgs 中的消息，发送心跳给别的节点
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Reject {
		// heartbeat 被拒绝的原因只可能是对方节点的 Term 更大
		r.becomeFollower(m.Term, None)
	} else {
		// 心跳同步成功

		// 检查该节点的日志是不是和自己是同步的，由于有些节点断开连接并又恢复了链接
		// 因此 leader 需要及时向这些节点同步日志
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

// handlePropose 追加从上层应用接收到的新日志，并广播给 follower
func (r *Raft) handlePropose(m pb.Message) {
	r.appendEntry(m.Entries)
	// leader 处于领导权禅让，停止接收新的请求
	if r.leadTransferee != None {
		return
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) == 1 {
		r.RaftLog.commit(r.RaftLog.LastIndex())
	} else {
		r.broadcastAppendEntry()
	}
}
func (r *Raft) appendEntry(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex() // leader 最后一条日志的索引
	for i := range entries {
		// 设置新日志的索引和任期
		entries[i].Index = lastIndex + uint64(i) + 1
		entries[i].Term = r.Term
		if entries[i].EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = entries[i].Index
		}
	}
	r.RaftLog.appendNewEntry(entries)
}

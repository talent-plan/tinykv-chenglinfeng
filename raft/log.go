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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	hardState, _, _ := storage.InitialState()

	rl := &RaftLog{
		storage:    storage,
		committed:  hardState.Commit,
		applied:    firstIndex - 1,
		stabled:    lastIndex,
		entries:    entries,
		dummyIndex: firstIndex,
	}

	return rl
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.LastIndex()-l.stabled == 0 {
		return make([]pb.Entry, 0)
	}
	return l.getEntries(l.stabled+1, 0)
}

// getEntries 返回 [start, end) 之间的所有日志，end = 0 表示返回 start 开始的所有日志
func (l *RaftLog) getEntries(start uint64, end uint64) []pb.Entry {
	if end == 0 {
		end = l.LastIndex() + 1
	}
	start, end = start-l.dummyIndex, end-l.dummyIndex
	return l.entries[start:end]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//fst applied=5 , committed=5 , dummyIndex=6
	//sec applied=5 , committed=10 , dummyIndex=6
	//want [6,7,8,9,10]
	//idx  [0,1,2,3,4 , 5) ===>[0,5)
	//diff = dummyIndex - 1 =5

	diff := l.dummyIndex - 1
	if l.committed > l.applied {
		return l.entries[l.applied-diff : l.committed-diff]
	}
	return make([]pb.Entry, 0)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.dummyIndex - 1 + uint64(len(l.entries))
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i >= l.dummyIndex {
		return l.entries[i-l.dummyIndex].Term, nil
	}
	// 2. 判断 i 是否等于当前正准备安装的快照的最后一条日志
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	// 3. 否则的话 i 只能是快照中的日志
	term, err := l.storage.Term(i)
	return term, err
}

// LastTerm 返回最后一条日志的索引
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	lastIndex := l.LastIndex() - l.dummyIndex
	return l.entries[lastIndex].Term
}

// 选举限制
func (l *RaftLog) isUpToDate(index, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

func (l *RaftLog) TermNoErr(i uint64) uint64 {
	//1.
	if i >= l.dummyIndex {
		return l.entries[i-l.dummyIndex].Term
	}
	//2.
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term
	}
	//3.debug here
	term, _ := l.storage.Term(i)
	return term
}

func (l *RaftLog) truncate(startIndex uint64) {
	if len(l.entries) > 0 {
		l.entries = l.entries[:startIndex-l.dummyIndex]
	}
}

// appendEntry 添加新的日志，并返回最后一条日志的索引
func (l *RaftLog) appendNewEntry(ents []*pb.Entry) uint64 {
	for i := range ents {
		l.entries = append(l.entries, *ents[i])
	}
	return l.LastIndex()
}

func (l *RaftLog) commit(toCommit uint64) {
	l.committed = toCommit
}

// maybeCommit 检查一个被大多数节点复制的日志是否需要提交
func (l *RaftLog) maybeCommit(toCommit, term uint64) bool {
	commitTerm, _ := l.Term(toCommit)
	if toCommit > l.committed && commitTerm == term {
		// 只有当该日志被大多数节点复制（函数调用保证），并且日志索引大于当前的commitIndex（Condition 1）
		// 并且该日志是当前任期内创建的日志（Condition 2），才可以提交这条日志
		// 【注】为了一致性，Raft 永远不会通过计算副本的方式提交之前任期的日志，只能通过提交当前任期的日志一并提交之前所有的日志
		l.commit(toCommit)
		return true
	}
	return false
}

package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const DebugCM = 1

type LogEntry struct {
	Command interface{}
	Term    int
}

type CMState int

const (
	Follower  CMState = iota // 跟随者
	Candidate                // 候选人
	Leader                   // 领导者
	Dead                     //
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

// 共识模块(CM)
type ConsensusModule struct {
	mu      sync.Mutex
	id      int     // id 是一致性模块中的服务器ID
	peerIds []int   // peerIds 是集群中所有同伴的ID列表
	server  *Server // server 是包含该CM的服务器. 该字段用于向其它同伴发起RPC调用

	currentTerm int        // 服务器接收到的最新任期（启动时初始化为0，单调递增）
	votedFor    int        // 在当前任期内收到赞同票的候选人ID（如果没有就是null）
	log         []LogEntry // 日志条目；每个条目中 包含输入状态机的指令，以及领导者接收条目时的任期（第一个索引是1）

	state              CMState
	electionResetEvent time.Time
}

// NewConsensusModule 使用给定的 ID、对等 ID 列表和
// 服务器创建一个新的 CM。就绪通道向 CM 发出信号，所有对等点都已连接，并且
// 可以安全地启动其状态机。
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1

	go func() {
		// CM 处于静止状态，直到发出就绪信号；然后，它开始倒计时以进行领导者选举。
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return cm
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Stop 停止这个 CM，清理它的状态。这个方法返回很快，但是
// 所有 goroutines 可能需要一些时间（直到 ~election timeout）
// 退出。
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("becomes Dead")
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

type RequestVoteArgs struct {
	Term         int // 候选人的任期
	CandidateId  int // 请求选票的候选人ID
	LastLogIndex int // 候选人的最新日志条目对应索引
	LastLogTerm  int // 候选人的最新日志条目对应任期
}

type RequestVoteReply struct {
	Term        int  // currentTerm，当前任期，回复给候选人。候选人用于自我更新
	VoteGranted bool // true表示候选人获得了赞成票
}

// 请求投票RPC RV请求
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	// 请求中的任期大于本地任期，转换为追随者状态
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	// 任期相同，且未投票或已投票给当前请求同伴，则返回赞成投票；否则，返回反对投票。
	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term     int // 领导者的任期
	LeaderId int // 领导者ID，追随者就可以对客户端进行重定向

	PrevLogIndex int        // 紧接在新日志条目之前的条目的索引
	PrevLogTerm  int        // prevLogIndex对应条目的任期
	Entries      []LogEntry // 需要报错的日志条目（为空时是心跳请求；为了高效可能会发送多条日志）
	LeaderCommit int        // 领导者的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm，当前任期，回复给领导者。领导者用于自我更新
	Success bool // 如果追随者保存了prevLogIndex和prevLogTerm相匹配的日志条目，则返回true
}

// AE请求 由领导者发起，用于向追随者复制客户端指令，也用于维护心跳
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)

	// 请求中的任期大于本地任期，转换为追随者状态
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		// 如果当前状态不是追随者，则变为追随者
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

//electionTimeout 生成一个伪随机的选举超时持续​​时间。
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// 如果设置了 RAFT_FORCE_MORE_REELECTION，则有意地进行压力测试
	// 经常生成一个硬编码的数字。这将在不同的服务器之间产生冲突
	// 并迫使更多的重新选举。
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// runElectionTimer 实现了一个选举计时器。它应该在
// 我们想要启动一个计时器以成为新选举的候选人时启动。
//
// 这个函数是阻塞的，应该在一个单独的 goroutine 中启动；
// 它旨在为单个（一次性）选举计时器工作，因为每当 CM 状态从跟随者/候选人或任期更改时它退出 。
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	/*
	  循环会在以下条件结束：
	  1 - 发现不再需要选举定时器
	  2 - 选举定时器超时，CM变为候选人
	  对于追随者而言，定时器通常会在CM的整个生命周期中一直在后台运行。
	*/
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		// CM不再需要定时器
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		// 任期变化
		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// 如果在超时之前没有收到领导者的信息或者给其它候选人投票，就开始新一轮选举
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

/*
如果追随者在一段时间内没有收到领导者或其它候选人的信息，它就会开始新一轮的选举。

将状态切换为候选人并增加任期，因为这是算法对每次选举的要求。
发送RV请求给其它同伴，请他们在本轮选举中为自己投票。
等待RPC请求的返回值，并统计我们是否获得足够多的票数成为领导者。
*/
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived int32 = 1

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}
			var reply RequestVoteReply

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)

				// 状态不是候选人，退出选举（可能退化为追随者，也可能已经胜选成为领导者）
				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state = %v", cm.state)
					return
				}

				// 存在更高任期（新领导者），转换为追随者
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes*2 > len(cm.peerIds)+1 {
							// 获得票数超过一半，选举获胜，成为最新的领导者
							cm.dlog("wins election with %d votes", votes)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	// 另行启动一个选举定时器，以防本次选举不成功
	go cm.runElectionTimer()
}

// becomeFollower 使 cm 成为跟随者并重置其状态。
// 期望 cm.mu 被锁定。
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm, cm.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// 只要当前服务器是领导者，就要周期性发送心跳
		for {
			cm.leaderSendHeartbeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	// 向所有追随者发送AE请求
	for _, peerId := range cm.peerIds {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}
		go func(peerId int) {
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				// 如果响应消息中的任期大于当前任期，则表明集群有新的领导者，转换为追随者
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}

package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const DebugCM = 1

/*
	CommitEntry就是Raft向提交通道发送的数据。每一条提交的条目都会通知客户端，
	表明指令已满足一致性，可以应用到客户端的状态机上。
*/
type CommitEntry struct {
	// Command 是被提交的客户端指令
	Command interface{}

	// Index 是被提交的客户端指令对应的日志索引
	Index int

	// Term 是被提交的客户端指令对应的任期
	Term int
}

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
	storage Storage

	commitChan         chan<- CommitEntry // commitChan 是此 CM 将报告已提交日志条目的通道。它是在施工过程中由客户传入的。
	newCommitReadyChan chan struct{}      // newCommitReadyChan 是 goroutines 使用的内部通知通道,它将新条目提交到日志以通知这些条目可能会在 commitChan 上发送。

	triggerAEChan chan struct{} // triggerAEChan 是一个内部通知通道，用于在发生有趣的变化时触发向关注者发送新的 AE。

	currentTerm int        // 服务器接收到的最新任期（启动时初始化为0，单调递增）
	votedFor    int        // 在当前任期内收到赞同票的候选人ID（如果没有就是null）
	log         []LogEntry // 日志条目；每个条目中 包含输入状态机的指令，以及领导者接收条目时的任期（第一个索引是1）

	commitIndex        int // 确认已提交的日志条目的最大索引值（初始化为0，单调递增）
	lastApplied        int // 应用于状态机的日志条目的最大索引值（初始化为0，单调递增）
	state              CMState
	electionResetEvent time.Time

	nextIndex  map[int]int // 对于每个服务器，存储要发送给该服务器的下一条日志条目的索引（初始化为领导者的最新日志索引+1）
	matchIndex map[int]int // 对于每个服务器，存储确认复制到该服务器的日志条目的最大索引值（初始化为0，单调递增）
}

// NewConsensusModule 使用给定的 ID、对等 ID 列表和
// 服务器创建一个新的 CM。就绪通道向 CM 发出信号，所有对等点都已连接，并且
// 可以安全地启动其状态机。
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	if cm.storage.HasData() {
		cm.restoreFromStorage(cm.storage)
	}

	go func() {
		// CM 处于静止状态，直到发出就绪信号；然后，它开始倒计时以进行领导者选举。
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()
	return cm
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

/*
	Submit方法会向CM呈递一条新的指令。这个函数是非阻塞的;
	客户端读取构造函数中传入的commit channel，以获得新提交条目的通知。
	如果当前CM是领导者返回true——表示指令被接受了。
	如果返回false，客户端会寻找新的服务器呈递该指令。
*/
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()

	cm.dlog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.persistToStorage()
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{}
		return true
	}

	cm.mu.Unlock()
	return false
}

// Stop 停止这个 CM，清理它的状态。这个方法返回很快，但是
// 所有 goroutines 可能需要一些时间（直到 ~election timeout）
// 退出。
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("becomes Dead")
	close(cm.newCommitReadyChan)
}

func (cm *ConsensusModule) restoreFromStorage(storage Storage) {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
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
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	// 请求中的任期大于本地任期，转换为追随者状态
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	// 任期相同，且未投票或已投票给当前请求同伴，则返回赞成投票；否则，返回反对投票。
	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.persistToStorage()
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

	ConflictIndex int //
	ConflictTerm  int
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

		// 检查本地的日志在索引PrevLogIndex处是否包含任期与PrevLogTerm匹配的记录？
		// 注意在PrevLogIndex=-1的极端情况下，这里应该是true
		if args.PrevLogIndex == -1 || (args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// 找到插入点 —— 索引从PrevLogIndex+1开始的本地日志与RPC发送的新条目间出现任期不匹配的位置。
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			/*
				循环结束时：
				- logInsertIndex指向本地日志结尾，或者是与领导者发送日志间存在任期冲突的索引位置
				- newEntriesIndex指向请求条目的结尾，或者是与本地日志存在任期冲突的索引位置
			*/
			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			if args.PrevLogIndex >= len(cm.log) {
				reply.ConflictIndex = len(cm.log)
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex points within our log, but PrevLogTerm doesn't match
				// cm.log[PrevLogIndex].
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
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
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
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

	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	/*
		该goroutine在后台运行并向同伴服务器发送AE请求：
		- triggerAEChan通道发送任何内容时
		- 如果triggerAEChan通道没有内容时，每50ms执行一次
	*/
	go func(heartbeatTimeout time.Duration) {
		// Immediately send AEs to peers.
		cm.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()

		// 只要当前服务器是领导者，就要周期性发送心跳
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true

				// Reset timer to fire again after heartbeatTimeout.
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-cm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				// Reset timer for heartbeatTimeout.
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
}

func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	// 向所有追随者发送AE请求
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			entries := cm.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						savedCommitIndex := cm.commitIndex
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}

						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, cm.nextIndex, cm.matchIndex, cm.commitIndex)
						if cm.commitIndex != savedCommitIndex {
							cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}
						}
					} else {
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := len(cm.log) - 1; i >= 0; i-- {
								if cm.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}
							if lastIndexOfTerm >= 0 {
								cm.nextIndex[peerId] = lastIndexOfTerm + 1
							} else {
								cm.nextIndex[peerId] = reply.ConflictIndex
							}
						} else {
							cm.nextIndex[peerId] = reply.ConflictIndex
						}
						cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}(peerId)
	}
}

/*
	commitChanSender负责在cm.commitChan上发送已提交的日志条目。
	它会监听newCommitReadyChan的通知并检查哪些条目可以发送（给客户端）。
	该方法应该在单独的后台goroutine中运行；cm.commitChan可能会有缓冲来限制客户端消费已提交指令的速度。
	当newCommitReadyChan关闭时方法结束。
*/
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		// Find which entries we have to apply.
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.dlog("commitChanSender done")
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

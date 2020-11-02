# 设计
* 一个 Raft 被一个 Service 依赖着，接受来自 Service 的 command，并在 command committed 后通知 Service apply command；
* 不用实现成员变更；
* 在 Lab 3 再实现日志压缩；
* 心跳 RPC 频率不能超过每秒 10 次，所需选举超时得大于 100ms；
* Leader 崩溃 5s 内需要选出一个新 Leader，所以选举超时需要设置得较短；
* 通过在循环中使用`time.Sleep`来实现 delay，不要使用`time.Timer`和`time.Ticker`？？？
*  [guide](https://thesquareplanet.com/blog/students-guide-to-raft/) / [locking](https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt) / [structure](https://pdos.csail.mit.edu/6.824/labs/raft-structure.txt)；

# Raft 接口
Raft 实现于 src/raft/raft.go：
```go
type Raft struct {
	peers      // 保存所有 peer（包括当前），所有 peer 上数组顺序相同
	persister  // 当前 peer 保存持久状态的地方
	me         // 当前 peer 的 index
  dead       // 当前 peer 是否启动
}

// 启动当前 peer
// applyCh: 通知 service apply command 的 channel
rf := Make(peers, me, persister, applyCh)

// 向 Raft 发送 command，应立即返回
rf.Start(command interface{}) (index, term, isleader)

// 获取 Raft 当前 term，并且反馈当前 peer 是否为 leader
rf.GetState() (term, isLeader)

// 发送 RequestVote RPC（非异步）
rf.sendRequestVote(server, args, reply)

// 接受处理 RequestVote RPC
rf.RequestVote(args, reply)

// 关闭当前 peer
rf.Kill()

// 当前 peer 是否已被关闭
rf.killed()

// 发送给 applyCh 的 meesage 类型
type ApplyMsg
```
由于每个 peer 中`peers`是固定的，peer 和 Raft 的关系不是“加入与否”的关系，而是“启动与否”的关系，当一个 peer 启动后 leader 会感知到然后向其发送心跳，所以 Raft 启动时不涉及成员变更的问题

每个 peer 是一个`*labrpc.ClientEnd`类型，提供了一个`Call`方法供`sendRequestVote`进行 RPC，`labrpc`包模拟了一种丢包网络

# RPC 接口
Raft 的 RPC 接口依赖于 src/labrpc：


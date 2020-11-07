# Part A

## 实验要求
* key 和 value 都是字符串；
* 服务支持三种操作：
	* `Put(key, value)`：替换`key`对应的`value`；
	* `Append(key, arg)`：将`arg`附加到`key`对应到`value`，如果不存在则等同于`Put`；
	* `Get(key)`：获取`key`对应的`value`，如果不存在返回空串；
* 执行一个操作前，需要确保之前都操作都已经落实了；
* 如果一个操作成功被应用，则通知客户端，否则向客户端报告一个错误，客户端应该重试其他服务器；
* 服务器之间不应该直接通信，只能通过 Raft 交换信息；
* 可以不实现论文 Section 8 中关于只读操作不写 log 的优化；
* 可以不实现服务器主动返回 leader，而在客户端对服务器遍历来寻找 leader；
* 可能出现这样的情况：当请求操作后，对应的服务器失去领导，新的 leader 接受了其他客户端的操作并提交，原来的服务器将新 leader 反馈的提交应用了，从而造成请求和响应不一致。可以在响应时判断服务器的任期是否变更，或者检查所响应的操作是否和请求对应上；
* 可以在客户端中记录最后通信的 leader，下次通信时优先考虑，从而节约寻找 leader 的时间；
* 当发生分区时，允许服务器和客户端无限期等待，直到分区恢复；
* Your scheme for duplicate detection should free server memory quickly, for example by having each RPC imply that the client has seen the reply for its previous RPC. It's OK to assume that a client will make only one call into a Clerk at a time？？？

# Part B

## 实验要求

* K/V Server 负责创建快照，并通知 Raft 丢弃日志；
* 
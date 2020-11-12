# 实验要求

![shard](images/shard.jpg)

* 每个 Replica Group 负责存储一组 K/V，通过 Raft 实现组内一致性；
* Shard Master 管理配置信息，决定如何分片，通过 Raft 实现容错；
* 客户端请求 Shard Master 来查询 Key 所在的 Replica Group；
* 分片需要在不同 Replica Group 之间移动，以实现 Replica Group 加入或离开集群时负载均衡；
* 负载均衡时，需要尽量少地移动分片；
* Shard Master 需要保留历史配置，不同的配置直接拥有唯一编号标示。第一个配置的编号为 0，不包含任何 Replica Group，所有的碎片都被分配到 GID 0（此时 GID 0 是无效的）；
* 分片的数量远多于 Replica Groups 数量，即每个 Replica Group 管理许多分片，以支持细粒度的负载转移；
* Shard Master 需要实现如下 RPC 为管理员控制提供接口：
	* `Join`：加入新 Replica Group；
	* `Leave`：将 Replica Group 从集群剔除；
	* `Move`：将分片移动到指定 Replica Group，单纯用于测试，之后的`Join`和`Leave`将覆盖`Move`的作用；
	* `Query`：查询特定编号的配置，如果参数为 -1 或大于最大编号，则返回最新配置；

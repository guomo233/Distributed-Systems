package shardkv


import "../shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"
import "time"
import "bytes"
import "log"

func init() {
	labgob.Register(shardmaster.Config{})
	labgob.Register(Pulled{})
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	Key string
	Value string
	ClerkId int64
	OpId int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied [shardmaster.NShards]map[int64]int64 // TODO clear
	applyFeedback map[int64](func())
	kv map[string]string
	nowIndex int
	persister *raft.Persister
	snapshotCh chan struct{}
	
	config   shardmaster.Config
	oldConfig shardmaster.Config
	mck *shardmaster.Clerk
	
	shifting bool
}

type Pulled struct {
	KV map[string]string
	Shards []int
	LastApplied [shardmaster.NShards]map[int64]int64
	ConfigNum int
}

type ShiftArgs struct {
	ConfigNum int
	Shards []int
}

type ShiftReply struct {
	Ok bool
	KV map[string]string
	LastApplied [shardmaster.NShards]map[int64]int64
}

func (kv *ShardKV) genSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kv)
	e.Encode(kv.lastApplied)
	e.Encode(kv.config)
	e.Encode(kv.oldConfig)
	e.Encode(kv.shifting)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) forceSnapshot() {
	kv.mu.Lock()
	snapshot := kv.genSnapshot()
	lastIncludedIndex := kv.nowIndex
	kv.mu.Unlock()
	
	kv.rf.Snapshot(lastIncludedIndex, snapshot)
}

func (kv *ShardKV) snapshot() {
	for {
		<- kv.snapshotCh
		
		for kv.maxraftstate > 0 &&
		   kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.mu.Lock()
			snapshot := kv.genSnapshot()
			lastIncludedIndex := kv.nowIndex
			kv.mu.Unlock()
			
			kv.rf.Snapshot(lastIncludedIndex, snapshot)
		}
	}
}

func (kv *ShardKV) checkSnapshot() {
	select {
	case kv.snapshotCh <- struct{}{}:
	default: // non-block
	}
}

func (kv *ShardKV) applyCommand(op Op) {
	if kv.applyFeedback[op.OpId] != nil {
		kv.applyFeedback[op.OpId]()
	}
	
	if kv.shifting || kv.config.Shards[key2shard(op.Key)] != kv.gid {
		return
	}
	
	if op.Op == "Put" || op.Op == "Append" {
		if kv.lastApplied[key2shard(op.Key)][op.ClerkId] == op.OpId {
			return
		}
		
		kv.lastApplied[key2shard(op.Key)][op.ClerkId] = op.OpId
		
		switch op.Op {
		case "Put":
			kv.kv[op.Key] = op.Value
		case "Append":
			kv.kv[op.Key] += op.Value
		}
	}
}

func (kv *ShardKV) applySnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvs map[string]string
	var lastApplied [shardmaster.NShards]map[int64]int64
	var config shardmaster.Config
	var oldConfig shardmaster.Config
	var shifting bool
	if d.Decode(&kvs) != nil ||
	   d.Decode(&lastApplied) != nil ||
	   d.Decode(&config) != nil ||
	   d.Decode(&oldConfig) != nil ||
	   d.Decode(&shifting) != nil {
		// TODO error...
	} else {
		kv.kv = kvs
		kv.lastApplied = lastApplied
		kv.config = config
		kv.oldConfig = oldConfig
		kv.shifting = shifting
	}
}

func (kv *ShardKV) registerFeedback(ch chan struct{}, opId int64, handler func()) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	kv.applyFeedback[opId] = func() {
		if handler != nil {
			handler()
		}
		ch <- struct{}{}
	}
}

func (kv *ShardKV) cancelFeedback(opId int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
		
	delete(kv.applyFeedback, opId)
}

func (kv *ShardKV) waitFeedback(ch chan struct{}, timeout int) bool {
	select {
		case <- ch:
			return true
		case <- time.After(time.Duration(timeout) * time.Millisecond):
			return false
	}
}

func (kv *ShardKV) applyPulled(pulled Pulled) {
	if !kv.shifting || pulled.ConfigNum != kv.config.Num {
		return
	}
	
	for k, v := range pulled.KV {
		kv.kv[k] = v
	}
	
	for _, shard := range pulled.Shards {
		for clerkId, opId := range pulled.LastApplied[shard] {
			kv.lastApplied[shard][clerkId] = opId
		}
	}
	
	kv.shifting = false
	log.Printf("[Server %d:%d] shift done %d\n", kv.gid, kv.me, kv.config.Num)
}

func (kv *ShardKV) ShiftShards(args *ShiftArgs, reply *ShiftReply) {
	kv.mu.Lock()
	
	if args.ConfigNum > kv.config.Num {
		log.Printf("[Server %d:%d] faild to shift shards: wait config update from %d to %d\n", kv.gid, kv.me, kv.config.Num, args.ConfigNum)
		kv.mu.Unlock()
		return
	}
	
	kv.mu.Unlock()
	
	opId := nrand()
	feedbackCh := make(chan struct{}, 1)
	kv.registerFeedback(feedbackCh, opId, func() {
		reply.KV = make(map[string]string)
		for _, shard := range args.Shards {
			for k, v := range kv.kv {
				if key2shard(k) == shard {
					reply.KV[k] = v
					reply.LastApplied[shard] = make(map[int64]int64)
					for clerkId, opId := range kv.lastApplied[shard] {
						reply.LastApplied[shard][clerkId] = opId
					}
				}
			}
		}
	})
	defer kv.cancelFeedback(opId)

	op := Op{Op: "Shift", OpId: opId}
	if _, _, ok := kv.rf.Start(op); !ok {
		log.Printf("[Server %d:%d] faild to shift shards: isn't leader\n", kv.gid, kv.me)
		return
	}

	if !kv.waitFeedback(feedbackCh, 300) {
		log.Printf("[Server %d:%d] faild to shift shards: timeout\n", kv.gid, kv.me)
		return
	}

	log.Printf("[Server %d:%d] shift shards %v\n", kv.gid, kv.me, reply.KV)
	reply.Ok = true
	return
}

func (kv *ShardKV) checkPull() {
	for {
		_, isLeader := kv.rf.GetState()
		
		kv.mu.Lock()
		
		if !kv.shifting || !isLeader {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		config := kv.config
		oldConfig := kv.oldConfig
		
		kv.mu.Unlock()
		
		tobePull := make(map[int][]int)
		for shard, gid := range config.Shards {
			source := oldConfig.Shards[shard]
			if gid == kv.gid &&
			   source != kv.gid {
				tobePull[source] = append(tobePull[source], shard)
			}
		}
		
		var pulled Pulled
		pulled.KV = make(map[string]string)
		pulled.ConfigNum = config.Num
		
		for gid, shards := range tobePull {
			if gid == 0 {
				continue
			}
			
			args := ShiftArgs{config.Num, shards}
			
			kv.mu.Lock()
			servers := oldConfig.Groups[gid]
			kv.mu.Unlock()
			
			// TODO effective: concurrent
			Loop:
			for {
				for _, server := range servers {
					srv := kv.make_end(server)
					
					var reply ShiftReply
					log.Printf("[Server %d:%d] pull shard %v from %d\n", kv.gid, kv.me, args, gid)
					srv.Call("ShardKV.ShiftShards", &args, &reply)
					if !reply.Ok {
						continue
					}
					
					for k, v := range reply.KV {
						pulled.KV[k] = v
					}
					
					for _, shard := range shards {
						pulled.LastApplied[shard] =
						 reply.LastApplied[shard]
					}
					
					pulled.Shards = append(pulled.Shards, shards...)
					
					break Loop
				}
				
				time.Sleep(100 * time.Millisecond)
			}
		}
		
		kv.rf.Start(pulled)
		
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applyConfig(config shardmaster.Config) {
	if kv.config.Num >= config.Num || kv.shifting {
		return
	}
	
	log.Printf("[Server %d:%d] change config from %v to %v, begin shift\n", kv.gid, kv.me, kv.config, config)
	
	kv.oldConfig = kv.config
	kv.config = config
	kv.shifting = true
}

func (kv *ShardKV) checkApply() {
	for {
		applyMsg := <-kv.applyCh
		
		kv.mu.Lock()
		
		if applyMsg.CommandValid {
			if op, ok := applyMsg.Command.(Op); ok {
				kv.applyCommand(op)
			} else if config, ok := 
			  applyMsg.Command.(shardmaster.Config); ok {
				kv.applyConfig(config)
			} else if pulled, ok :=
			  applyMsg.Command.(Pulled); ok {
				kv.applyPulled(pulled)
			}
		} else {
			kv.applySnapshot()
		}
		
		kv.nowIndex = applyMsg.CommandIndex
		
		kv.mu.Unlock()
		
		kv.checkSnapshot()
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opId := nrand()
	feedbackCh := make(chan struct{}, 1)
	kv.registerFeedback(feedbackCh, opId, func() {
		if kv.shifting ||
		   kv.config.Shards[key2shard(args.Key)] != kv.gid {
			log.Printf("[Server %d:%d] reject request Get(%s): wrong group\n", kv.gid, kv.me, args.Key)
			reply.Err = ErrWrongGroup
			return
		}
		reply.Value = kv.kv[args.Key]
		reply.Err = OK
		log.Printf("[Server %d:%d] done request Get(%s): %s\n", kv.gid, kv.me, args.Key, reply.Value)
	})
	defer kv.cancelFeedback(opId)
	
	op := Op{Op: "Get", OpId: opId}
	if _, _, ok := kv.rf.Start(op); !ok {
		log.Printf("[Server %d:%d] reject request Get(%s): isn't leader\n", kv.gid, kv.me, args.Key)
		reply.Err = ErrWrongLeader
		return
	}
	
	if !kv.waitFeedback(feedbackCh, 300) {
		log.Printf("[Server %d:%d] reject request Get(%s): timeout\n", kv.gid, kv.me, args.Key)
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	
	if kv.lastApplied[key2shard(args.Key)][args.ClerkId] == args.OpId {
		log.Printf("[Server %d:%d] request[%d:%d] %s(%s, %s) have been done before\n", kv.gid, kv.me, args.ClerkId, args.OpId, args.Op, args.Key, args.Value)
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	
	kv.mu.Unlock()
	
	feedbackCh := make(chan struct{}, 1)
	kv.registerFeedback(feedbackCh, args.OpId, func() {
		if kv.shifting ||
		   kv.config.Shards[key2shard(args.Key)] != kv.gid {
			log.Printf("[Server %d:%d] reject request[%d:%d] %s(%s, %s): wrong group\n", kv.gid, kv.me, args.ClerkId, args.OpId , args.Op, args.Key, args.Value)
			reply.Err = ErrWrongGroup
			return
		}
		reply.Err = OK
		log.Printf("[Server %d:%d] done request[%d:%d] %s(%s, %s)\n", kv.gid, kv.me, args.ClerkId, args.OpId, args.Op, args.Key, args.Value)
	})
	defer kv.cancelFeedback(args.OpId)
	
	op := Op{args.Op, args.Key, args.Value, args.ClerkId, args.OpId}
	if _, _, ok := kv.rf.Start(op); !ok {
		log.Printf("[Server %d:%d] reject request[%d:%d] %s(%s, %s): isn't leader\n", kv.gid, kv.me, args.ClerkId, args.OpId, args.Op, args.Key, args.Value)
		reply.Err = ErrWrongLeader
		return
	}
	
	if !kv.waitFeedback(feedbackCh, 300) {
		log.Printf("[Server %d:%d] reject request[%d:%d] %s(%s, %s): timeout\n", kv.gid, kv.me, args.ClerkId, args.OpId, args.Op, args.Key, args.Value)
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) fetchConfig() {
	for {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		kv.mu.Lock()
		configNum := kv.config.Num
		kv.mu.Unlock()
		
		config := kv.mck.Query(configNum + 1)
		
		kv.mu.Lock()
		changed := config.Num > kv.config.Num && !kv.shifting
		kv.mu.Unlock()
		
		if changed {
			log.Printf("[Server %d:%d] fetch config: %v\n", kv.gid, kv.me, config)
			kv.rf.Start(config)
		}
		
		time.Sleep(100 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	// TODO
	log.Printf("[Server %d:%d] dead\n", kv.gid, kv.me)
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.applyFeedback = make(map[int64](func()))
	kv.kv = make(map[string]string)
	kv.persister = persister
	kv.snapshotCh = make(chan struct{}, 1)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.lastApplied[i] = make(map[int64]int64)
	}

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.applySnapshot()
	
	go kv.checkApply()
	go kv.snapshot()
	go kv.fetchConfig()
	go kv.checkPull()
	
	log.Printf("[Server %d:%d] start\n", kv.gid, kv.me)

	return kv
}

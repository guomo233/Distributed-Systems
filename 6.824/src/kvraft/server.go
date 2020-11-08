package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied map[int64]int64 // TODO clear
	applyFeedback map[int64](func())
	kv map[string]string
	nowIndex int
	persister *raft.Persister
}

func (kv *KVServer) applyCommand(op Op) {
	
	if op.Op != "Get" {
        if kv.lastApplied[op.ClerkId] == op.OpId {
			return
		}

		kv.lastApplied[op.ClerkId] = op.OpId
		
		switch op.Op {
		case "Put":
			log.Printf("[Server %d] apply request[%d:%d] Put(%s, %s)\n", kv.me, op.ClerkId, op.OpId, op.Key, op.Value)
			kv.kv[op.Key] = op.Value
		case "Append":
			log.Printf("[Server %d] apply request[%d:%d] Append(%s, %s)\n", kv.me, op.ClerkId, op.OpId, op.Key, op.Value)
			kv.kv[op.Key] += op.Value
		}
	}
	
	if kv.applyFeedback[op.OpId] != nil {
		kv.applyFeedback[op.OpId]()
	}
}

func (kv *KVServer) checkApply() {
	for {
		applyMsg := <-kv.applyCh
		
		kv.mu.Lock()
		
		op := applyMsg.Command.(Op)
		kv.applyCommand(op)
		
		kv.nowIndex = applyMsg.CommandIndex
		
		kv.mu.Unlock()
	}
}

func (kv *KVServer) registerFeedback(ch chan struct{}, opId int64, handler func()) {
	kv.applyFeedback[opId] = func() {
		delete(kv.applyFeedback, opId)
		if handler != nil {
			handler()
		}
		ch <- struct{}{}
	}
}

func (kv *KVServer) cancelFeedback(opId int64) {
	delete(kv.applyFeedback, opId)
}

func (kv *KVServer) waitFeedback(ch chan struct{}, timeout int) bool {
	select {
		case <- ch:
			return true
		case <- time.After(time.Duration(timeout) * time.Millisecond):
			return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opId := nrand()
	
	feedbackCh := make(chan struct{}, 1)
	
	kv.mu.Lock()
	kv.registerFeedback(feedbackCh, opId, func() {
		reply.Value = kv.kv[args.Key]
	})
	kv.mu.Unlock()
	
	op := Op{OpId: opId}
	if _, _, ok := kv.rf.Start(op); !ok {
		log.Printf("[Server %d] reject request Get(%s): isn't leader\n", kv.me, args.Key)
		kv.mu.Lock()
		kv.cancelFeedback(opId)
		kv.mu.Unlock()
		
		reply.Err = ErrWrongLeader
		return
	}
	
	if !kv.waitFeedback(feedbackCh, 300) {
		log.Printf("[Server %d] reject request Get(%s): apply timeout\n", kv.me, args.Key)
		reply.Err = ErrWrongLeader
		return
	}
	
	reply.Err = OK
	log.Printf("[Server %d] done request Get(%s): %s\n", kv.me, args.Key, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	feedbackCh := make(chan struct{}, 1)
	
	kv.mu.Lock()
	
	if kv.lastApplied[args.ClerkId] == args.OpId {
		log.Printf("[Server %d] reject request[%d:%d] %s(%s, %s): isn't leader\n", kv.me, args.ClerkId, args.OpId , args.Op, args.Key, args.Value)
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	
	kv.registerFeedback(feedbackCh, args.OpId, nil)
	
	kv.mu.Unlock()
	
	op := Op{args.Op, args.Key, args.Value, args.ClerkId, args.OpId}
	if _, _, ok := kv.rf.Start(op); !ok {
		log.Printf("[Server %d] reject request[%d:%d] %s(%s, %s): isn't leader\n", kv.me, args.ClerkId, args.OpId, args.Op, args.Key, args.Value)
		kv.mu.Lock()
		kv.cancelFeedback(args.OpId)
		kv.mu.Unlock()
		
		reply.Err = ErrWrongLeader
		return
	}
	
	if !kv.waitFeedback(feedbackCh, 300) {
		log.Printf("[Server %d] reject request[%d:%d] %s(%s, %s): apply timeout\n", kv.me, args.ClerkId, args.OpId, args.Op, args.Key, args.Value)
		reply.Err = ErrWrongLeader
		return
	}
	
	reply.Err = OK
	log.Printf("[Server %d] done request[%d:%d] %s(%s, %s)\n", kv.me, args.ClerkId, args.OpId, args.Op, args.Key, args.Value)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.lastApplied = make(map[int64]int64)
	kv.applyFeedback = make(map[int64](func()))
	kv.kv = make(map[string]string)
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.checkApply()
	
	return kv
}

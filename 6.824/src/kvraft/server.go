package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"runtime"
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
	Id int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	applied map[int64]bool
	applyFeedback map[int64](chan struct{})
	kv map[string]string
}

// DEBUG
func runFuncName()string{
	pc := make([]uintptr,1)
	runtime.Callers(2,pc)
	f := runtime.FuncForPC(pc[0])
	return f.Name()
}

func (kv *KVServer) checkApply() {
	for {
		op := (<-kv.applyCh).Command.(Op)
		
		kv.mu.Lock()
		
		if kv.applied[op.Id] {
			kv.mu.Unlock()
			continue
		}
		
		kv.applied[op.Id] = true
		switch op.Op {
		case "Put":
			log.Printf("[Server %d] apply request[%d] Put(%s, %s)\n", kv.me, op.Id, op.Key, op.Value)
			kv.kv[op.Key] = op.Value
		case "Append":
			log.Printf("[Server %d] apply request[%d] Append(%s, %s)\n", kv.me, op.Id, op.Key, op.Value)
			kv.kv[op.Key] += op.Value
		}
		
		if kv.applyFeedback[op.Id] != nil {
			kv.applyFeedback[op.Id] <- struct{}{}
		}
		
		kv.mu.Unlock()
	}
}

func (kv *KVServer) waitApply(id int64, timeout int) bool {
	kv.mu.Lock()
	applyFeedback := kv.applyFeedback[id]
	kv.mu.Unlock()

	var res bool
	select {
		case <- applyFeedback:
			res = true
		case <- time.After(time.Duration(timeout) * time.Millisecond):
			res = false
	}
	
	kv.mu.Lock()
	delete(kv.applyFeedback, id)
	kv.mu.Unlock()
	
	return res
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	id := nrand()
	
	kv.mu.Lock()
	kv.applyFeedback[id] = make(chan struct{}, 1)
	kv.mu.Unlock()
	
	op := Op{Id: id}
	if _, _, ok := kv.rf.Start(op); !ok {
		log.Printf("[Server %d] reject request Get(%s): isn't leader\n", kv.me, args.Key)
		kv.mu.Lock()
		delete(kv.applyFeedback, op.Id)
		kv.mu.Unlock()
		
		reply.Err = ErrWrongLeader
		return
	}
	
	if !kv.waitApply(op.Id, 300) {
		log.Printf("[Server %d] reject request Get(%s): apply timeout\n", kv.me, args.Key)
		reply.Err = ErrWrongLeader
		return	
	}
	
	kv.mu.Lock()
	reply.Value = kv.kv[args.Key]
	kv.mu.Unlock()
	
	reply.Err = OK
	log.Printf("[Server %d] done request Get(%s): %s\n", kv.me, args.Key, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	
	if kv.applied[args.Id] {
		log.Printf("[Server %d] reject request[%d] %s(%s, %s): isn't leader\n", kv.me, args.Id , args.Op, args.Key, args.Value)
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	
	kv.applyFeedback[args.Id] = make(chan struct{}, 1)
	
	kv.mu.Unlock()
	
	op := Op{args.Op, args.Key, args.Value, args.Id}
	if _, _, ok := kv.rf.Start(op); !ok {
		log.Printf("[Server %d] reject request[%d] %s(%s, %s): isn't leader\n", kv.me, args.Id , args.Op, args.Key, args.Value)
		kv.mu.Lock()
		delete(kv.applyFeedback, op.Id)
		kv.mu.Unlock()
		
		reply.Err = ErrWrongLeader
		return
	}
	
	if !kv.waitApply(op.Id, 300) {
		log.Printf("[Server %d] reject request[%d] %s(%s, %s): apply timeout\n", kv.me, args.Id , args.Op, args.Key, args.Value)
		reply.Err = ErrWrongLeader
		return
	}
	
	reply.Err = OK
	log.Printf("[Server %d] done request[%d] %s(%s, %s)\n", kv.me, args.Id, args.Op, args.Key, args.Value)
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
	kv.applied = make(map[int64]bool)
	kv.applyFeedback = make(map[int64](chan struct{}))
	kv.kv = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.checkApply()

	return kv
}

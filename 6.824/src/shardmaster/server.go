package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "bytes"
import "time"
import "log"
import "sort"

type groupSlot struct {
	gid int
	valid bool
	shards []int
}

type byShardsNum []groupSlot

func (s byShardsNum) Len() int { return len(s)}
	
func (s byShardsNum) Less(i, j int) bool {
	if s[i].valid != s[j].valid {
		return s[j].valid
	}
	return len(s[i].shards) > len(s[j].shards)
}

func (s byShardsNum) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	slots []groupSlot
	configs []Config // indexed by config num
	lastApplied map[int64]int64 // TODO clear
	applyFeedback map[int64](func())
	nowIndex int
	persister *raft.Persister
	snapshotCh chan struct{}
	maxraftstate int
}

type Op struct {
	// Your data here.
	Op string
	JoinServers map[int][]string
	LeaveGIDs []int
	MoveShard int
	MoveGID   int
	ClerkId int64
	OpId int64
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (sm *ShardMaster) lastedConfig() *Config {
	return &sm.configs[len(sm.configs) - 1]
}

func (sm *ShardMaster) genSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.lastApplied)
	e.Encode(sm.slots)
	data := w.Bytes()
	return data
}

func (sm *ShardMaster) snapshot() {
	for {
		<- sm.snapshotCh
		
		for sm.maxraftstate > 0 &&
		   sm.persister.RaftStateSize() >= sm.maxraftstate {
			sm.mu.Lock()
			snapshot := sm.genSnapshot()
			lastIncludedIndex := sm.nowIndex
			sm.mu.Unlock()
			
			sm.rf.Snapshot(lastIncludedIndex, snapshot)
		}
	}
}

func (sm *ShardMaster) checkSnapshot() {
	select {
	case sm.snapshotCh <- struct{}{}:
	default: // non-block
	}
}

func (sm *ShardMaster) balance() {
	sort.Sort(byShardsNum(sm.slots))
	
	var invalidSlots []groupSlot
	for i := 0; i < len(sm.slots); i++ {
		if sm.slots[i].valid == true {
			invalidSlots = sm.slots[:i]
			sm.slots = sm.slots[i:]
			break
		}
	}
	
	limMin := NShards / len(sm.slots)
	limMax := limMin
	if limMin * len(sm.slots) < NShards {
		limMax++
	}
	
	var nMax int
	for nMax = 0; nMax <= len(sm.slots); nMax++ {
		nMin := len(sm.slots) - nMax
		if nMin * limMin + nMax * limMax == NShards {
			break
		}
	}
	
	iHunger := 0
	for _, invalidSlot := range invalidSlots {
		for len(invalidSlot.shards) > 0 {
			var nIn int
			if iHunger < nMax {
				nIn = limMax - len(sm.slots[iHunger].shards)
			} else {
				nIn = limMin - len(sm.slots[iHunger].shards)
			}
			
			if nIn <= 0 {
				iHunger++
				continue
			}
			
			nMove := min(len(invalidSlot.shards), nIn)
			sm.slots[iHunger].shards = append(sm.slots[iHunger].shards, invalidSlot.shards[:nMove]...)
			invalidSlot.shards = invalidSlot.shards[nMove:]
		}
	}
	
	for iFull := range sm.slots {
		var nOut int
		if iFull < nMax {
			nOut = len(sm.slots[iFull].shards) - limMax
		} else {
			nOut = len(sm.slots[iFull].shards) - limMin
		}
		
		for nOut > 0 {
			var nIn int
			if iHunger < nMax {
				nIn = limMax - len(sm.slots[iHunger].shards)
			} else {
				nIn = limMin - len(sm.slots[iHunger].shards)
			}
			
			if nIn <= 0 {
				iHunger++
				continue
			}
			
			nMove := min(nOut, nIn)
			sm.slots[iHunger].shards = append(sm.slots[iHunger].shards, sm.slots[iFull].shards[:nMove]...)
			sm.slots[iFull].shards = sm.slots[iFull].shards[nMove:]
			nOut -= nMove
		}
	}
}

func (sm *ShardMaster) join(servers map[int][]string) {
	shards := sm.lastedConfig().Shards
	groups := make(map[int][]string)
	for gid, servers := range sm.lastedConfig().Groups {
		groups[gid] = servers
	}
	
	for gid, servers := range servers {
		if groups[gid] == nil {
			groups[gid] = servers
			sm.slots = append(sm.slots, groupSlot{gid: gid, valid: true})
		}
	}
	
	if len(groups) == len(sm.lastedConfig().Groups) {
		return
	}
	
	sm.balance()
	
	for _, slot := range sm.slots {
		for _, shard := range slot.shards {
			shards[shard] = slot.gid
		}
	}
	
	sm.configs = append(sm.configs, Config{
		sm.lastedConfig().Num + 1,
		shards,
		groups,
	})
}

func (sm *ShardMaster) leave(gids []int) {
	shards := sm.lastedConfig().Shards
	groups := make(map[int][]string)
	for gid, servers := range sm.lastedConfig().Groups {
		groups[gid] = servers
	}
	
	for _, gid := range gids {
		delete(groups, gid)
	}
	
	if len(groups) == len(sm.lastedConfig().Groups) {
		return
	}
	
	for i := range sm.slots {
		if groups[sm.slots[i].gid] == nil {
			sm.slots[i].valid = false
		}
	}
	
	sm.balance()
	
	for _, slot := range sm.slots {
		for _, shard := range slot.shards {
			shards[shard] = slot.gid
		}
	}
	
	sm.configs = append(sm.configs, Config{
		sm.lastedConfig().Num + 1,
		shards,
		groups,
	})
}

func (sm *ShardMaster) move(shard, gid int) {
	if shard < 0 || shard >= NShards ||
	   sm.lastedConfig().Groups[gid] == nil ||
	   sm.lastedConfig().Shards[shard] == gid {
		return
	}
	
	shards := sm.lastedConfig().Shards
	groups := make(map[int][]string)
	for gid, servers := range sm.lastedConfig().Groups {
		groups[gid] = servers
	}
	
	var iOld, iNew int
	for i := range sm.slots {
		if sm.slots[i].gid == shards[shard] {
			iOld = i
		} else if sm.slots[i].gid == gid {
			iNew = i
		}
	}
	
	for iMove := 0; iMove < len(sm.slots[iOld].shards); iMove++ {
		if sm.slots[iOld].shards[iMove] == shard {
			length := len(sm.slots[iOld].shards)
			sm.slots[iOld].shards[iMove], 
				sm.slots[iOld].shards[length - 1] =
				sm.slots[iOld].shards[length - 1], 
				sm.slots[iOld].shards[iMove]
			sm.slots[iOld].shards = 
				sm.slots[iOld].shards[:length - 1]
			break
		}
	}
	
	sm.slots[iNew].shards = append(sm.slots[iNew].shards, shard)
	
	shards[shard] = gid
	
	sm.configs = append(sm.configs, Config{
		sm.lastedConfig().Num + 1,
		shards,
		groups,
	})
}

func (sm *ShardMaster) applyCommand(op Op) {
	if op.Op != "Query" {
		if sm.lastApplied[op.ClerkId] == op.OpId {
			return
		}
		
		sm.lastApplied[op.ClerkId] = op.OpId
		
		switch op.Op {
		case "Join":
			log.Printf("join %v\n", op.JoinServers)
			sm.join(op.JoinServers)
			log.Printf("new config: %v\n", sm.lastedConfig())
		case "Leave":
			log.Printf("leave %v\n", op.LeaveGIDs)
			sm.leave(op.LeaveGIDs)
			log.Printf("new config: %v\n", sm.lastedConfig())
		case "Move":
			sm.move(op.MoveShard, op.MoveGID)
		}
	}
	
	if sm.applyFeedback[op.OpId] != nil {
		sm.applyFeedback[op.OpId]()
	}
}

func (sm *ShardMaster) applySnapshot() {
	snapshot := sm.persister.ReadSnapshot()
	
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var configs []Config
	var lastApplied map[int64]int64
	var slots []groupSlot
	if d.Decode(&configs) != nil ||
	   d.Decode(&lastApplied) != nil ||
	   d.Decode(&slots) != nil {
		// TODO error...
	} else {
		sm.configs = configs
		sm.lastApplied = lastApplied
		sm.slots = slots
	}
}

func (sm *ShardMaster) registerFeedback(ch chan struct{}, opId int64, handler func()) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	sm.applyFeedback[opId] = func() {
		if handler != nil {
			handler()
		}
		ch <- struct{}{}
	}
}

func (sm *ShardMaster) cancelFeedback(opId int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
		
	delete(sm.applyFeedback, opId)
}

func (sm *ShardMaster) waitFeedback(ch chan struct{}, timeout int) bool {
	select {
		case <- ch:
			return true
		case <- time.After(time.Duration(timeout) * time.Millisecond):
			return false
	}
}

func (sm *ShardMaster) checkApply() {
	for {
		applyMsg := <-sm.applyCh
		
		sm.mu.Lock()
		
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			sm.applyCommand(op)
		} else {
			sm.applySnapshot()
		}
		
		sm.nowIndex = applyMsg.CommandIndex
		
		sm.mu.Unlock()
		
		sm.checkSnapshot()
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()
	
	if sm.lastApplied[args.ClerkId] == args.OpId {
		sm.mu.Unlock()
		return
	}
	
	sm.mu.Unlock()
	
	feedbackCh := make(chan struct{}, 1)
	sm.registerFeedback(feedbackCh, args.OpId, nil)
	defer sm.cancelFeedback(args.OpId)
	
	op := Op {
		Op: "Join",
		JoinServers: args.Servers,
		ClerkId: args.ClerkId,
		OpId: args.OpId,
	}
	
	if _, _, ok := sm.rf.Start(op); !ok {
		reply.WrongLeader = true
		return
	}
	
	if !sm.waitFeedback(feedbackCh, 300) {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()
	
	if sm.lastApplied[args.ClerkId] == args.OpId {
		sm.mu.Unlock()
		return
	}
	
	sm.mu.Unlock()
	
	feedbackCh := make(chan struct{}, 1)
	sm.registerFeedback(feedbackCh, args.OpId, nil)
	defer sm.cancelFeedback(args.OpId)
	
	op := Op {
		Op: "Leave",
		LeaveGIDs: args.GIDs,
		ClerkId: args.ClerkId,
		OpId: args.OpId,
	}
	
	if _, _, ok := sm.rf.Start(op); !ok {
		reply.WrongLeader = true
		return
	}
	
	if !sm.waitFeedback(feedbackCh, 300) {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	
	if sm.lastApplied[args.ClerkId] == args.OpId {
		sm.mu.Unlock()
		return
	}
	
	sm.mu.Unlock()
	
	feedbackCh := make(chan struct{}, 1)
	sm.registerFeedback(feedbackCh, args.OpId, nil)
	defer sm.cancelFeedback(args.OpId)
	
	op := Op {
		Op: "Move",
		MoveGID: args.GID,
		MoveShard: args.Shard, 
		ClerkId: args.ClerkId, 
		OpId: args.OpId,
	}
	
	if _, _, ok := sm.rf.Start(op); !ok {
		reply.WrongLeader = true
		return
	}
	
	if !sm.waitFeedback(feedbackCh, 300) {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	opId := nrand()
	
	feedbackCh := make(chan struct{}, 1)
	sm.registerFeedback(feedbackCh, opId, func() {
		if args.Num == -1 || args.Num >= len(sm.configs) {
			reply.Config = *sm.lastedConfig()
		} else {
			reply.Config = sm.configs[args.Num]
		}
	})
	defer sm.cancelFeedback(opId)
	
	op := Op{Op: "Query", OpId: opId}
	if _, _, ok := sm.rf.Start(op); !ok {
		reply.WrongLeader = true
		return
	}
	
	if !sm.waitFeedback(feedbackCh, 300) {
		reply.WrongLeader = true
		return
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.lastApplied = make(map[int64]int64)
	sm.applyFeedback = make(map[int64](func()))
	sm.persister = persister
	sm.maxraftstate = -1
	sm.snapshotCh = make(chan struct{}, 1)
	
	var shards []int
	for shard := 0; shard < NShards; shard++ {
		shards = append(shards, shard)
	}
	sm.slots = append(sm.slots, groupSlot{0, false, shards})

	sm.applySnapshot()
	
	go sm.checkApply()
	go sm.snapshot()

	return sm
}

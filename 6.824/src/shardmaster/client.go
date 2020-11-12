package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader int
	clerkId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clerkId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		nServers := len(ck.servers)
		for i := 0; i < nServers; i++ {
			var reply QueryReply
			ok := ck.servers[ck.leader].Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
			ck.leader = (ck.leader + 1) % nServers
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{ClerkId: ck.clerkId, OpId: nrand()}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		nServers := len(ck.servers)
		for i := 0; i < nServers; i++ {
			var reply JoinReply
			ok := ck.servers[ck.leader].Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			ck.leader = (ck.leader + 1) % nServers
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{ClerkId: ck.clerkId, OpId: nrand()}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		nServers := len(ck.servers)
		for i := 0; i < nServers; i++ {
			var reply LeaveReply
			ok := ck.servers[ck.leader].Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			ck.leader = (ck.leader + 1) % nServers
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{ClerkId: ck.clerkId, OpId: nrand()}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		nServers := len(ck.servers)
		for i := 0; i < nServers; i++ {
			var reply MoveReply
			ok := ck.servers[ck.leader].Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			ck.leader = (ck.leader + 1) % nServers
		}
		time.Sleep(100 * time.Millisecond)
	}
}

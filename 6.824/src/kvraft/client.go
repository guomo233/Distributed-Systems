package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "log"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
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
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// TODO optimize: no log
	for {
		nServers := len(ck.servers)
		for i := 0; i < nServers; i++ {
			args := GetArgs{key}
			reply := GetReply{}
			log.Printf("[Client] send request Get(%s) to server %d\n", key, ck.leader)
			ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
			if reply.Err == OK {
				log.Printf("[Client] got reply Get(%s) from server %d: %s\n", key, ck.leader, reply.Value)
				return reply.Value
			}

			ck.leader = (ck.leader + 1) % nServers
		}
		
		time.Sleep(500 * time.Millisecond)
		log.Printf("[Client] retry request Get(%s)\n", key)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	opId := nrand()
	
	for {
		nServers := len(ck.servers)
		for i := 0; i < nServers; i++ {
			args := PutAppendArgs{key, value, op, opId}
			reply := PutAppendReply{}
			log.Printf("[Client] send request[%d] %s(%s, %s) to server %d\n", args.Id, op, key, value, ck.leader)
			ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
			if reply.Err == OK {
				log.Printf("[Client] got reply[%d] %s(%s, %s) from server %d\n", args.Id, op, key, value, ck.leader)
				return
			}
			
			ck.leader = (ck.leader + 1) % nServers
		}
		
		time.Sleep(500 * time.Millisecond)
		log.Printf("[Client] retry request[%d] %s(%s, %s)\n", opId, op, key, value)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	curLeader int
	id int64
	serial int
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
	ck.curLeader = 0
	ck.id = nrand()
	ck.serial = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	//index := ck.curLeader

	args := GetArgs{
		Key: key,
		ClientId: ck.id,
		Serial: ck.serial,
	}
	for {
		reply := GetReply{}

		ok := ck.servers[ck.curLeader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader{
			return reply.Value
		} else {
			ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
		}
		time.Sleep(100*time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key: key,
		Value: value,
		ClientId: ck.id,
		Serial: ck.serial,
	}
	ck.serial ++
	for {
		reply := PutAppendReply{}

		ok := true
		if op == "Put"{
			ok = ck.servers[ck.curLeader].Call("KVServer.Put", &args, &reply)
		} else {
			ok = ck.servers[ck.curLeader].Call("KVServer.Append", &args, &reply)
		}
		if ok && reply.Err != ErrWrongLeader{
			return 
		} else {
			ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
		}
		time.Sleep(100*time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

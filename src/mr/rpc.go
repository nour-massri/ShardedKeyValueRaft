package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Define a type for the enumeration
type TaskType int

// Define constants to represent different Task Types
const (
    Map    TaskType = iota // iota is a predeclared identifier representing the successive untyped integer constants
    Reduce
    Exit
	Stall
)
// Add your RPC definitions here.

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Tasktype TaskType
	Tasknum int
	Nreduce int
	Nmap int
	Filename []string
}

type UpdateCoordinatorArgs struct {
	Tasktype TaskType
	Tasknum int
}
type UpdateCoordinatorReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Define a type for the enumeration
type TaskStatus int

// Define constants to represent different Task Types
const (
    Idle    TaskStatus = iota // iota is a predeclared identifier representing the successive untyped integer constants
    Inprogress
    Completed
)

type Task struct{
	status TaskStatus
	fileName string
	tasktime time.Time
}

type Coordinator struct {
	// Your definitions here.
	// coordinator state here
	mu sync.Mutex
	nmap int
	nreduce int
	maptasks []Task
	maptasksleft int
	reducetasks []Task
	reducetasksleft int
}

// Your code here -- RPC handlers for the worker to call.


func IsLongerThan10(tasktime time.Time) bool {
	duration := time.Now().Sub(tasktime)
	return duration.Seconds() > 10
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	//give task or stall worker

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maptasksleft > 0{//give map task or nothing
		//fmt.Println("getmaptask\n")
		for i:=0;i<c.nmap;i++{
			if c.maptasks[i].status == Idle || (c.maptasks[i].status == Inprogress && IsLongerThan10(c.maptasks[i].tasktime)){
				c.maptasks[i].status = Inprogress
				c.maptasks[i].tasktime = time.Now()
				*reply = GetTaskReply{Map, i, c.nreduce, c.nmap, []string{c.maptasks[i].fileName}}
				return nil
			}
		}
		//nothing 
		*reply = GetTaskReply{Stall, 0, c.nreduce, c.nmap, []string{}}
	} else if c.reducetasksleft > 0 {//give reduce task or nothing
		//fmt.Println("getreducetask\n")

		for i:=0;i<c.nreduce;i++{
			if c.reducetasks[i].status == Idle || (c.reducetasks[i].status == Inprogress && IsLongerThan10(c.reducetasks[i].tasktime)){
				c.reducetasks[i].status = Inprogress
				c.reducetasks[i].tasktime = time.Now()
				*reply = GetTaskReply{Reduce, i, c.nreduce, c.nmap, []string{}}
				return nil
			}
		}
		//nothing
		*reply = GetTaskReply{Stall, 0, c.nreduce, c.nmap, []string{}}
	}else {
		*reply = GetTaskReply{Exit, 0, c.nreduce, c.nmap, []string{}}
	}
	return nil
}
func (c *Coordinator) UpdateCoordinator(args *UpdateCoordinatorArgs, reply *UpdateCoordinatorReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Tasktype == Map{
		if c.maptasks[args.Tasknum].status != Inprogress{
			return nil
		}
		c.maptasks[args.Tasknum].status = Completed
		c.maptasksleft -- 
		fmt.Printf("maptasksleft %v\n", c.maptasksleft)
	} else{
		if c.reducetasks[args.Tasknum].status != Inprogress{
			return nil
		}
		c.reducetasks[args.Tasknum].status = Completed
		c.reducetasksleft --
		fmt.Printf("reducetasksleft %v\n", c.reducetasksleft)

	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reducetasksleft == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mu.Lock()
	c.nmap = len(files)
	for i:=0;i<c.nmap;i++{
		c.maptasks = append(c.maptasks, Task{Idle, files[i], time.Now()})
	}
	c.nreduce = nReduce
	for i:=0;i<c.nreduce;i++{
		c.reducetasks = append(c.reducetasks, Task{Idle,"", time.Now()})
	}
	c.maptasksleft = c.nmap
	c.reducetasksleft = c.nreduce
	//count time for in progress?
	//how to identify workeers 1,2,3
	//how to do atomoically save file name in worker
	c.mu.Unlock()
	c.server()
	return &c
}

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % Nreduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func MapTask(mapf func(string, string) []KeyValue,
			 task GetTaskReply){
	filename := task.Filename[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	reducebuckets := make([][]KeyValue, task.Nreduce)
	for _, kv := range kva{
		reducebuckets[ihash(kv.Key)%task.Nreduce] = append(reducebuckets[ihash(kv.Key)%task.Nreduce], kv)
	}
	for i := 0; i < task.Nreduce; i++{
		oname := fmt.Sprintf("mr-%d-%d",task.Tasknum ,i)
		ofile, _ := os.CreateTemp("", "temp")
		enc := json.NewEncoder(ofile)
		for _, kv := range reducebuckets[i] {
		  err := enc.Encode(&kv)
		  if err != nil{
			log.Fatalf("cannot encode k,v pair %v", kv)
		  }
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
}

func ReduceTask(reducef func(string, []string) string,
			 task GetTaskReply){
	
	intermediate := []KeyValue{}
	for i := 0; i < task.Nmap; i++ {
		filename := fmt.Sprintf("mr-%d-%d",i ,task.Tasknum)

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
		  var kv KeyValue
		  if err := dec.Decode(&kv); err != nil {
			break
		  }
		  intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.Tasknum)
	ofile, _ := os.CreateTemp("", "temp")
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	os.Rename(ofile.Name(), oname)
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	for {
		task := GetTask()
		if task.Tasktype == Map{
			MapTask(mapf, task)
			UpdateCoordinator(task.Tasktype, task.Tasknum)
		} else if task.Tasktype == Reduce{
			ReduceTask(reducef, task)
			UpdateCoordinator(task.Tasktype, task.Tasknum)
		} else if task.Tasktype == Exit{
			os.Exit(0)
		} else if task.Tasktype == Stall {
			time.Sleep(2 * time.Second)		}
		//in a loop, ask the coordinator for a task, 
		//read the task's input from one or more files,
		// execute the task, write the task's output to one or more files,
		// and again ask the coordinator for a new task.
	}
	// Your worker implementation here.
	
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetTask() GetTaskReply{

	// declare an argument structure.
	args := GetTaskArgs{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := GetTaskReply{}
	
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply:Tasktype: %v taksnum: %v filename:%v\n", reply.Tasktype, reply.Tasknum, reply.Filename)
	} else {
		fmt.Printf("gettask failed!\n")
	}
	return reply
}

func UpdateCoordinator(	Tasktype TaskType, Tasknum int) UpdateCoordinatorReply{

	// declare an argument structure
	args := UpdateCoordinatorArgs{Tasktype, Tasknum}

	// fill in the argument(s).

	// declare a reply structure.
	reply := UpdateCoordinatorReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.UpdateCoordinator", &args, &reply)
	if ok {
		fmt.Printf("completed tasktype: %v tasknum: %v\n", Tasktype, Tasknum)
	} else {
		fmt.Printf("UpdateCoordinator failed!\n")
	}
	return reply
}



//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
var workerSockName string
var waitTilDone chan bool

type MapCounts struct {
	//Partitions int
	State workerState
}

// MapState
type MapState struct {
	//M piece
	task           Task
	mapReduceTasks []string
	counts         MapCounts
}

type ReducerCounts struct {
	State workerState
	//number of unique intermediate Keys
	Keys int
	// total number of intermediate Keys
	Values int
}

type ReducerState struct {
	//R partition
	intermediate []KeyValue
	reduceId     int
	counts       ReducerCounts
}

func (ms *MapState) Snapshot(taskId TaskId, reply *MapCounts) error {
	if taskId != TaskId(ms.task.TaskId) {
		return fmt.Errorf("Expected %d taskId but got %d. Check state on master.\n", ms.task.TaskId, taskId)
	}
	*reply = ms.counts
	if ms.counts.State == completed {
		waitTilDone <- true
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (ms *MapState) server(sockname string) {
	rpc.Register(ms)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v\n", sockname, e)
	}
	go http.Serve(l, nil)
}

func (rs *ReducerState) Snapshot(reduceId int, reply *ReducerCounts) error {
	if reduceId != rs.reduceId {
		return fmt.Errorf("Expected %d reduceId but got %d. Check state on master.\n", rs.reduceId, reduceId)
	}
	*reply = rs.counts
	if rs.counts.State == completed {
		waitTilDone <- true
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (rs *ReducerState) server(sockname string) {
	rpc.Register(rs)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v\n", sockname, e)
	}
	//fmt.Printf("Worker listening at %s\n", sockname)
	go http.Serve(l, nil)
}

// main/mrworker.go calls this function.
// A reasonable naming convention for intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	waitTilDone = make(chan bool)
	coordSockName = sockname
	workerSockName = coordinatorSock()
	phase := os.Getenv("PHASE")

	if len(phase) == 0 || phase == "0" {
		fmt.Printf("Starting worker in map phase with PID %d\n", os.Getpid())
		c, err := connectrpc(coordSockName)
		if err != nil {
			fmt.Printf(fmt.Errorf("Client Connection Failed: %v\n", err).Error())
			return
		}
		ms := MapState{}
		// for heartbeat messages between coordinator and worker
		func() { ms.server(workerSockName) }()
		err = callrpc(c, "Coordinator.GetmTask", WorkerInfo{
			WorkerId: WorkerId(os.Getpid()),
			Type:     0,
			Sockname: workerSockName,
		}, &ms.task)
		defer c.Close()
		if err != nil {
			fmt.Printf(fmt.Errorf("Worker Id %d: %v\n", ms.task.TaskId, err).Error())
			return
		}
		//do work with Task
		file, err := os.Open(ms.task.Path)
		if err != nil {
			log.Fatalf("cannot open %v\n", ms.task.Path)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v\n", ms.task.Path)
		}
		file.Close()
		kva := mapf(ms.task.Path, string(content))

		fps := make([]*os.File, 0, ms.task.ReduceTasks)
		ms.mapReduceTasks = make([]string, 0, ms.task.ReduceTasks)

		for partition := range ms.task.ReduceTasks {
			writeTo := fmt.Sprintf("mr-%d-%d", ms.task.TaskId, partition)
			fp, err := os.OpenFile(writeTo, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				log.Fatalf("cannot open %v\n", writeTo)
			}
			ms.mapReduceTasks = append(ms.mapReduceTasks, writeTo)
			fps = append(fps, fp)
		}

		//TODO: use a combiner function --use a flag and compare memory usage and performance metrics with or without it
		// TODO: map[partition]count
		for _, kv := range kva {
			partition := ihash(kv.Key) % ms.task.ReduceTasks
			writeTo := ms.mapReduceTasks[partition]
			fp := fps[partition]
			enc := json.NewEncoder(fp)
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to buffered Key/Value Pair to %v\n", writeTo)
			}
		}

		reply := -1
		err = callrpc(c, "Coordinator.Notify", NotifyTask{
			MapTask:     ms.task,
			WorkerId:    WorkerId(os.Getpid()),
			ReduceTasks: ms.mapReduceTasks,
		}, &reply)

		if err != nil {
			fmt.Printf("Worker %d failed to Notify Task %d: %v\n", os.Getpid(), ms.task.TaskId, err)
			return
		}
		ms.counts.State = completed
		<-waitTilDone
	} else {
		fmt.Printf("Starting worker in reduce phase with PID %d\n", os.Getpid())
		c, err := connectrpc(coordSockName)
		if err != nil {
			fmt.Printf(fmt.Errorf("Client Connection Failed: %v\n", err).Error())
			return
		}

		// store Task in rs just like the map
		var task RTask
		// for heartbeat messages between coordinator and worker
		rs := ReducerState{intermediate: []KeyValue{}}
		func() { rs.server(workerSockName) }()

		err = callrpc(c, "Coordinator.GetrTask", WorkerInfo{
			WorkerId: WorkerId(os.Getpid()),
			Type:     1,
			Sockname: workerSockName,
		}, &task)

		c.Close()

		if err != nil {
			fmt.Printf(fmt.Errorf("Worker Id %d: %v\n", task.ReduceId, err).Error())
			return
		}
		rs.reduceId = task.ReduceId

		//fmt.Println(task.MapOutputs)
		//per mapTask
		for _, reducerTask := range task.MapOutputs {
			file, err := os.Open(reducerTask)
			if err != nil {
				log.Fatal(err)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				rs.intermediate = append(rs.intermediate, kv)
			}
			file.Close()
		}
		sort.Sort(ByKey(rs.intermediate))
		// combiner code
		i := 0
		writeTo := fmt.Sprintf("mr-out-%d", task.ReduceId)
		ofile, err := os.OpenFile(writeTo, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

		if err != nil {
			log.Fatal("Failed to open file\n")
		}

		for i < len(rs.intermediate) {
			j := i + 1 // number of key count ?
			//compares the current and subsequent keys and increment j if they are the same
			for j < len(rs.intermediate) && rs.intermediate[j].Key == rs.intermediate[i].Key {
				j++
			}
			values := []string{}
			//saves them in value
			for k := i; k < j; k++ {
				values = append(values, rs.intermediate[k].Value)
			}
			output := reducef(rs.intermediate[i].Key, values)
			rs.counts.Keys++
			rs.counts.Values += len(values)
			fmt.Fprintf(ofile, "%v %v\n", rs.intermediate[i].Key, output)
			// this is the correct format for each line of Reduce output.
			i = j
		}
		rs.counts.State = completed
		ofile.Close()
		<-waitTilDone
	}
}

// send an RPC request to the coordinator, wait for the response.
func callrpc(c *rpc.Client, rpcname string, arg any, reply any) error {
	err := c.Call(rpcname, arg, reply)
	//blocking
	if err != nil {
		return err
	}
	return nil
}

func connectrpc(sockName string) (*rpc.Client, error) {
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		return nil, err
	}
	return c, nil
}

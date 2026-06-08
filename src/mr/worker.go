package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var activeMaxMapTasks int = 8
var failedConns int = 0

type taskState int

// 2. Use iota to auto-increment values
const (
	active taskState = iota
	idle
)

type workerTask struct {
	state   taskState
	payload Task
}

type workerTasks struct {
	tasks []workerTask
}

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

// main/mrworker.go calls this function.
// A reasonable naming convention for intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	var tasks workerTasks = workerTasks{}
	var mu sync.Mutex

	notifyReducerCh := make(chan struct {
		mapTask TaskId
		output  []string
	})

	var done bool = false
	// This go routine will check the status of the job
	go func(status *bool) {
		connRetries := 0
		rpcRetries := 0
		for {
			if *status {
				break
			}
			if connRetries == 5 {
				*status = true
			}
			c, err := connectrpc()
			if err != nil {
				fmt.Printf(fmt.Errorf("Client Connection Failed for Job Status Check: %v\n", err).Error())
				connRetries++
				// How many retries before we give up
				continue
			}
			err = callrpc(c, "Coordinator.JobStatus", 0, status)
			if err != nil {
				fmt.Printf(fmt.Errorf("RPC Failed: %v\n", err).Error())
				// How many retries before we give up
				rpcRetries++
				c.Close()
				continue
			}
		}
	}(&done)
	//waiting for finished mapTasks
	// when do the worker process return ??
	go func() {
		// reducers
		for {
			reduceNotif := <-notifyReducerCh
			go func() {
				defer func() {
					// how do we recover from crashes??
				}()
				for _, reducerTask := range reduceNotif.output {
					var reduceKey int
					var task int
					intermediate := []KeyValue{}
					fmt.Sscanf(reducerTask, "mr-%d-%d", &task, &reduceKey)
					//assert task==reduceNotif.task
					if task != int(reduceNotif.mapTask) {
						log.Fatal("ReduceKeys Mismatch")
					}
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
						intermediate = append(intermediate, kv)
					}
					file.Close()
					i := 0
					for i < len(intermediate) {
						j := i + 1 // number of key count ?
						//compares the current and subsequent keys and increment j if they are the same
						for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
							j++
						}
						values := []string{}
						//saves them in value
						for k := i; k < j; k++ {
							values = append(values, intermediate[k].Value)
						}
						output := reducef(intermediate[i].Key, values)
						writeTo := fmt.Sprintf("mr-out-%d", reduceKey)
						mu.Lock()
						ofile, err := os.OpenFile(writeTo, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
						if err != nil {
							log.Fatal(err)
						}
						fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
						mu.Unlock()
						ofile.Close()
						// this is the correct format for each line of Reduce output.
						i = j
					}
				}
			}()
		}
	}()

	connRetriesMapTasks := 0
	// Worker
	for {
		if done {
			//till we figure when the worker process exits, as the mapTasks , reduceTasks lifecyle are tied together
			for {
			}
		}
		if connRetriesMapTasks == 5 {
			for {
			}
		}
		c, err := connectrpc()
		if err != nil {
			fmt.Printf(fmt.Errorf("Client Connection Failed: %v\n", err).Error())
			connRetriesMapTasks++
			// How many retries before we give up
			continue
		}
		connRetriesMapTasks = 0
		var path Task
		taskId := len(tasks.tasks)
		err = callrpc(c, "Coordinator.GetTask", taskId, &path)

		if err != nil {
			c.Close()
			fmt.Printf(fmt.Errorf("Task Id %d: %v\n", taskId, err).Error())
			connRetriesMapTasks++
			continue
			// How many retries before we give up
		}
		fmt.Printf("Spawning Map Worker Task %d\n", taskId)
		tasks.tasks = append(tasks.tasks, workerTask{
			payload: path,
			state:   active,
		})
		go func(taskId TaskId, path Task) {
			defer func() {
				//c.Close()
			}()
			//do work with Task
			file, err := os.Open(path.Path)
			if err != nil {
				log.Fatalf("cannot open %v", path.Path)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", path.Path)
			}
			file.Close()
			kva := mapf(path.Path, string(content))
			sort.Sort(ByKey(kva))
			mapReduceTasks := []string{}
			for _, kv := range kva {
				writeTo := fmt.Sprintf("mr-%d-%d", taskId, ihash(kv.Key)%path.ReduceTasks)
				//create reduceTasks
				reduceTaskFile, err := os.OpenFile(writeTo, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					log.Fatalf("cannot open %v", writeTo)
				}
				enc := json.NewEncoder(reduceTaskFile)
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write to buffered Key/Value Pair to %v", writeTo)
				}
				mapReduceTasks = append(mapReduceTasks, writeTo)
				reduceTaskFile.Close()
			}
			notifyReducerCh <- struct {
				mapTask TaskId
				output  []string
			}{
				mapTask: taskId,
				output:  mapReduceTasks,
			}
		}(TaskId(taskId), path)
		c.Close()
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

func connectrpc() (*rpc.Client, error) {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return nil, err
	}
	return c, nil
}

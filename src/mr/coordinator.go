package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// Contains book keeping of the whole job, number of tasks completed etc
// I think we need a mutax to lock access to the coordinator but for now we keep things simple and make iteractive client conns
// TODO: implement a stack interface for "tasks"
type Coordinator struct {
	mu sync.Mutex //
	// Your definitions here.
	files    []string
	finished struct {
		files []string
		count int
	}
	pending struct {
		files []string
		count int
	}
}

func (c *Coordinator) GetTask(arg TaskId, reply *Task) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if c.pending.count == 0 {
        return errors.New("EEMPTY")
    }

    taskIndex := c.pending.count - 1

    *reply = Task{
        Path: c.pending.files[taskIndex],
    }

    c.pending.count--

    fmt.Printf(
        "Client Worker Task %d just started with %s\n",
        arg,
        reply.Path,
    )

    return nil
}

func (c *Coordinator) FinishedTask(arg TaskId, reply *Task) error {
	return nil
}

// Give status updates about finished map tasks...if a map task fails to respond for some time
// We can give out it task
func (c *Coordinator) TStatus(arg Args, reply *TaskStatus) error {
	return nil
}

func (c *Coordinator) JobStatus(arg Args, reply *JobStatus) error {
	if c.pending.count == 0{
		*reply=true
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.pending.count == 0
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	fmt.Printf("listening at %s\n", sockname)
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// The Coordinator tellsthe worker process how many threads to use for the
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, pending: struct {
		files []string
		count int
	}{ files: files, count: len(files)}, finished: struct {
		files []string
		count int
	}{}}

	// Your code here.

	c.server(sockname)
	return &c
}

package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"sync"
	"time"
)

type workerState int
type channelType int

const (
	in_progress workerState = iota
	completed
	failed
)

const (
	scheduler channelType = iota
	monitor
	cdone
)

type taskStatus struct {
	//workerId WorkerId
	state workerState
	info  WorkerInfo
}

// TODO: Read/Write locks for mTasks and rTasks
type Coordinator struct {
	mTasks *[][]taskStatus
	//M
	mInputs []string
	//R
	mOutput *[][]string
	rTasks  *[][]taskStatus
	mu      sync.Mutex
}

type workerChannelItem struct {
	info   WorkerInfo
	taskId int
}
type workerTasks struct {
	completed chan workerChannelItem
	pending   chan workerChannelItem
	failed    chan workerChannelItem
}

var reduceTasksCnt int
var completedRTasks = 0

// It's use have a similar behavior like a sem
var availWorkers chan int

var quitScheduler chan bool

type channelTasks [3]workerTasks

var workerChannels channelTasks

// idle/non-active/nil -> in_progress
func (c *Coordinator) GetrTask(arg WorkerInfo, reply *RTask) error {
	schedulerChannel := workerChannels[scheduler]
	c.mu.Lock()
	defer c.mu.Unlock()
	// TODO: markTaskProgress
	for reduceTask := range reduceTasksCnt {
		rTask := (*c.rTasks)[reduceTask]
		if rTask == nil {
			*reply = RTask{
				MapOutputs: (*c.mOutput)[reduceTask],
				ReduceId:   reduceTask,
			}
			(*c.rTasks)[reduceTask] = []taskStatus{{info: arg, state: in_progress}}
			schedulerChannel.pending <- workerChannelItem{info: arg, taskId: reduceTask}
			return nil
		} else {
			if rTask[len(rTask)-1].state == failed {
				c.markTask(1, reduceTask, arg, in_progress)
				*reply = RTask{
					MapOutputs: (*c.mOutput)[reduceTask],
					ReduceId:   reduceTask,
				}
				schedulerChannel.pending <- workerChannelItem{info: arg, taskId: reduceTask}
				return nil
			}
		}
	}

	// TODO; change this for backup Tasks (3.6)
	return fmt.Errorf("All rtasks are inprogress\n")
}

// idle/non-active/nil -> in_progress
func (c *Coordinator) GetmTask(arg WorkerInfo, reply *Task) error {
	schedulerChannel := workerChannels[scheduler]
	c.mu.Lock()
	defer c.mu.Unlock()
	for mapTask, file := range c.mInputs {
		mTask := (*c.mTasks)[mapTask]
		if mTask == nil {
			(*c.mTasks)[mapTask] = []taskStatus{
				{info: arg, state: in_progress},
			}
			*reply = Task{
				Path:        file,
				ReduceTasks: reduceTasksCnt,
				TaskId:      mapTask,
			}
			schedulerChannel.pending <- workerChannelItem{info: arg, taskId: mapTask}
			return nil
		} else {
			lastStatus := mTask[len(mTask)-1]
			if lastStatus.state == failed {
				c.markTask(0, mapTask, arg, in_progress)
				*reply = Task{
					Path:        file,
					ReduceTasks: reduceTasksCnt,
					TaskId:      mapTask,
				}
				schedulerChannel.pending <- workerChannelItem{info: arg, taskId: mapTask}
				return nil
			}
		}
	}

	//TODO: change this for backup Tasks (3.6)
	return fmt.Errorf("All tasks are inprogress\n")
}

// in_progress -> completed
func (c *Coordinator) Notify(arg NotifyTask, reply *ReceivedNotofication) error {
	if reduceTasksCnt != len(arg.ReduceTasks) {
		*reply = -1 //INSUF_RT
		return fmt.Errorf("Expected %d but got %d\n", reduceTasksCnt, len(arg.ReduceTasks))
	}

	<-availWorkers
	c.mu.Lock()
	defer c.mu.Unlock()

	info := WorkerInfo{WorkerId: arg.WorkerId, Type: 0}
	c.markTask(0, arg.MapTask.TaskId, info, completed)
	*reply = 0

	// Produce a single reduce task per partition
	// TODO: we can schedule reduce workers from here. In the paper reduce and map workers run concurrently.

	// recordReducePartitions
	for index, reduceTask := range arg.ReduceTasks {
		reduceTasks := (*c.mOutput)[index]
		if reduceTasks == nil {
			(*c.mOutput)[index] = make([]string, 0, reduceTasksCnt)
			reduceTasks = (*c.mOutput)[index]
		}
		reduceTasks = append(reduceTasks, reduceTask)
		(*c.mOutput)[index] = reduceTasks
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.

// 0 - cc in completed R tasks
func (c *Coordinator) Done() bool {
	ch := workerChannels[cdone].completed
	if completedRTasks == reduceTasksCnt {
		quitScheduler <- true
		return true
	}
	<-ch
	completedRTasks++
	return false
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v\n", sockname, e)
	}
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// TODO: 3.6 (Backup Tasks)
// TODO: 4.2 (Ordering Guarantees)
// TODO: 4.3 (Combiner Function)
// TODO: 4.8 (Status Information)
// TODO: 4.9 (Counters)
// TODO: implement task locking, remove global lock
// TODO: implement tasks methods

func (wt channelTasks) initializePending(ch channelType) {
	wt[ch].pending = make(chan workerChannelItem)
}
func (wt channelTasks) initializeCompleted(ch channelType) {
	wt[ch].completed = make(chan workerChannelItem)
}
func (wt channelTasks) initializeFailed(ch channelType) {
	wt[ch].failed = make(chan workerChannelItem)
}

func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {

	M := len(files)
	reduceTasksCnt = nReduce
	availWorkers = make(chan int, 4)
	workerChannels.initializeCompleted(cdone)
	quitScheduler = make(chan bool)

	var mapTasks [][]taskStatus = make([][]taskStatus, M)
	var mapOutput [][]string = make([][]string, nReduce)
	var reduceTasks [][]taskStatus = make([][]taskStatus, nReduce)
	c := Coordinator{mTasks: &mapTasks, rTasks: &reduceTasks, mOutput: &mapOutput, mInputs: files}
	c.server(sockname)

	//scheduler
	//1 - interested in completed Tasks
	//2 - interested in failed tasks
	//3 - interested in-progress tasks
	go func(mSize int, rSize int) {

		workerChannels.initializePending(scheduler)
		workerChannels.initializeCompleted(scheduler)
		//local completedMTasks and completedRTasks..one thread have access, no locks
		compTasks := 0
		inprogressTasks := 0
		failedTasks := 0
		totalTasks := mSize
		phase := 0
		for {
			select {
			// the above changes for backup tasks
			// wait for a worker returning
			case availWorkers <- 1:
				if (compTasks + inprogressTasks) == totalTasks {
					time.Sleep(1 * time.Second)
					continue
				}
				args := []string{"wc.so", sockname}
				//TODO: handle test cases as test runs in src/mr
				cmd := exec.Command("./mrworker", args...)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr

				if compTasks == mSize {
					phase = 1
					totalTasks = rSize
					compTasks = 0
					inprogressTasks = 0
				}
				cmd.Env = []string{fmt.Sprintf("PHASE=%d", phase), "GOCACHE=/tmp/go-cache"}
				if err := cmd.Start(); err != nil {
					log.Fatal(err)
				}
				if cmd.Process == nil {
					<-availWorkers
					continue
				}
			//receieve task and forward it to interested goroutines based on certain conditions
			case t := <-workerChannels[scheduler].pending:
				inprogressTasks++
				workerChannels[monitor].pending <- t
			case t := <-workerChannels[scheduler].completed:
				compTasks++
				if phase == 1 {
					workerChannels[cdone].completed <- t
				}
				c.mu.Lock()
				c.markTask(t.info.Type, t.taskId, t.info, completed)
				c.mu.Unlock()
				<-availWorkers
			case t := <-workerChannels[scheduler].failed:
				failedTasks++
				c.mu.Lock()
				c.markTask(t.info.Type, t.taskId, t.info, failed)
				c.mu.Unlock()
				<-availWorkers
			case <-quitScheduler:
				//clean up
				return
			}
		}
	}(M, nReduce)

	go c.workerMonitor()

	return &c
}

func (c *Coordinator) markTask(typ int, taskId int, info WorkerInfo, state workerState) {
	var tasks *[][]taskStatus
	if typ == 0 {
		tasks = c.mTasks
	} else {
		tasks = c.rTasks
	}
	// read critical sections
	task := (*tasks)[taskId]

	// write crital sections
	task = append(task, taskStatus{info: info, state: state})
	(*tasks)[taskId] = task
}

func (c *Coordinator) workerMonitor() {
	workerChannels.initializePending(monitor)
	for {
		worker := <-workerChannels[monitor].pending
		client, err := connectrpc(worker.info.Sockname)

		if err != nil {
			workerChannels[scheduler].failed <- worker
			continue
		}

		go func(client *rpc.Client, worker workerChannelItem) {
			done := make(chan *rpc.Call, 1)
			for {
				timeout := time.After(10 * time.Second)
				if worker.info.Type == 0 {
					counts := MapCounts{}
					client.Go("MapState.Snapshot", worker.taskId, &counts, done)
					select {
					case <-timeout:
						workerChannels[scheduler].failed <- worker
						return
					case c := <-done:
						if c.Error != nil {
							fmt.Printf("%v\n", c.Error)
							workerChannels[scheduler].failed <- worker
							return
						}
						reply := c.Reply.(*MapCounts)
						if reply.State == completed {
							workerChannels[scheduler].completed <- worker
							return
						}
					}
				} else {
					counts := ReducerCounts{}
					client.Go("ReducerState.Snapshot", worker.taskId, &counts, done)
					select {
					case <-timeout:
						workerChannels[scheduler].failed <- worker
						return
					case c := <-done:
						if c.Error != nil {
							fmt.Printf("%v\n", c.Error)
							workerChannels[scheduler].failed <- worker
							return
						}
						reply := c.Reply.(*ReducerCounts)
						if reply.State == completed {
							workerChannels[scheduler].completed <- worker
							return
						}
					}
				}
				time.Sleep(1 * time.Second)
			}
		}(client, worker)
	}
}

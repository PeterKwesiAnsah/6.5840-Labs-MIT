package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskId int
type Args int
type WorkerId int
type WorkerInfo struct {
	WorkerId int
	Sockname string
	Type     int
}

type TaskStatus bool
type JobStatus bool

type Task struct {
	Path   string
	TaskId int
	//number of reduce Tasks to reproduce
	ReduceTasks int
}
type RTask struct {
	MapOutputs []string
	ReduceId   int
}

type ReceivedNotofication int
type NotifyTask struct {
	MapTask     Task
	WorkerId    WorkerId
	ReduceTasks []string
}

// Add your RPC definitions here.

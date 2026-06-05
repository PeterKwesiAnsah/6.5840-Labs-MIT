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

type TaskStatus bool
type JobStatus bool

type Task struct{ Path string }

// Add your RPC definitions here.

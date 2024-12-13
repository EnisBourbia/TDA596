package mr

import (
	"os"
	"strconv"
)

// KeyValue is a type used to hold key/value pairs.
type KeyValue struct {
	Key   string
	Value string
}

// TaskRequest is sent by a worker to request a task.
type TaskRequest struct {
	WorkerID int
}

// TaskReply contains the details of the task assigned to the worker.
type TaskReply struct {
	Type      string // "Map", "Reduce", or "Wait"
	TaskID    int
	FileName  string   // For map tasks
	NReduce   int      // Number of reduce tasks
	NMap      int      // Number of map tasks
	Completed bool     // Indicates if all tasks are completed
}

// TaskCompletion is used by a worker to notify the coordinator of task completion.
type TaskCompletion struct {
	Type    string // "Map" or "Reduce"
	TaskID  int
	WorkerID int
}

// EmptyReply is used when no response data is needed.
type EmptyReply struct{}

// coordinatorSock generates a unique UNIX-domain socket name for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

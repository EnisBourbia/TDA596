package mr

type TaskRequest struct {
	WorkerID int
	Address  string // Worker's network address
}

type TaskResponse struct {
	TaskType       string
	FileName       string
	InputData      string // New field
	TaskID         int
	NReduce        int
	NFiles         int
	AllDone        bool
	WorkerID       int
	MapWorkerAddrs map[int]string // MapTaskID to Worker Address
}

type TaskCompleteArgs struct {
	TaskType string
	TaskID   int
	WorkerID int
}

type RegisterWorkerArgs struct {
	Address string
}

type RegisterWorkerReply struct {
	WorkerID int
}

type GetIntermediateDataArgs struct {
	MapTaskID    int
	ReduceTaskID int
}

type GetIntermediateDataReply struct {
	Data []KeyValue
}

// Coordinator address function (modify as needed)
func coordinatorSock() string {
	return ":1234" // Coordinator listens on this port
}

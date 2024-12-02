package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	FileName   string
	TaskType   string
	State      TaskState
	WorkerID   int
	StartTime  time.Time
	WorkerAddr string // Store the worker's network address
}

type Coordinator struct {
	MapTasks     []Task
	ReduceTasks  []Task
	NReduce      int
	NMap         int
	AllTasksDone bool
	mu           sync.Mutex

	WorkerAddrs  map[int]string // Map WorkerID to their network address
	NextWorkerID int
}

// AssignTask provides available tasks to workers.
func (c *Coordinator) AssignTask(req *TaskRequest, res *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.AllTasksDone {
		res.AllDone = true
		return nil
	}

	// Update worker address
	c.WorkerAddrs[req.WorkerID] = req.Address

	// Assign a map task if any are available
	for i, task := range c.MapTasks {
		if task.State == Idle {
			c.MapTasks[i].State = InProgress
			c.MapTasks[i].WorkerID = req.WorkerID
			c.MapTasks[i].StartTime = time.Now()
			c.MapTasks[i].WorkerAddr = req.Address

			res.TaskType = "map"
			res.TaskID = i
			res.FileName = task.FileName
			res.NReduce = c.NReduce
			res.WorkerID = req.WorkerID
			return nil
		}
	}

	// Assign a reduce task if all map tasks are completed
	mapDone := true
	for _, task := range c.MapTasks {
		if task.State != Completed {
			mapDone = false
			break
		}
	}
	if mapDone {
		for i, task := range c.ReduceTasks {
			if task.State == Idle {
				c.ReduceTasks[i].State = InProgress
				c.ReduceTasks[i].WorkerID = req.WorkerID
				c.ReduceTasks[i].StartTime = time.Now()

				// Collect map workers' addresses for reduce task
				mapWorkerAddrs := make(map[int]string)
				for idx, mapTask := range c.MapTasks {
					mapWorkerAddrs[idx] = mapTask.WorkerAddr
				}

				res.TaskType = "reduce"
				res.TaskID = i
				res.NReduce = c.NReduce
				res.NFiles = c.NMap
				res.WorkerID = req.WorkerID
				res.MapWorkerAddrs = mapWorkerAddrs
				return nil
			}
		}
	}

	// No tasks are ready; inform worker to wait
	res.TaskType = "wait"
	return nil
}

// MarkTaskCompleted marks a task as completed.
func (c *Coordinator) MarkTaskCompleted(args *TaskCompleteArgs, _ *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		if c.MapTasks[args.TaskID].WorkerID == args.WorkerID {
			c.MapTasks[args.TaskID].State = Completed
		}
	} else if args.TaskType == "reduce" {
		if c.ReduceTasks[args.TaskID].WorkerID == args.WorkerID {
			c.ReduceTasks[args.TaskID].State = Completed
		}
	}

	c.checkAllTasksDone()
	return nil
}

// RegisterWorker registers a new worker and assigns a unique WorkerID.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.NextWorkerID++
	workerID := c.NextWorkerID
	c.WorkerAddrs[workerID] = args.Address
	reply.WorkerID = workerID
	return nil
}

// checkAllTasksDone verifies if all tasks are completed.
func (c *Coordinator) checkAllTasksDone() {
	mapDone := true
	for _, task := range c.MapTasks {
		if task.State != Completed {
			mapDone = false
			break
		}
	}

	if mapDone {
		reduceDone := true
		for _, task := range c.ReduceTasks {
			if task.State != Completed {
				reduceDone = false
				break
			}
		}
		c.AllTasksDone = reduceDone
	}
}

// monitorTasks reassigns timed-out tasks.
func (c *Coordinator) monitorTasks() {
	for {
		time.Sleep(1000 * time.Millisecond)
		c.mu.Lock()
		for i, task := range c.MapTasks {
			if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
				c.MapTasks[i].State = Idle
				log.Printf("Map task %d timed out. Reassigning...\n", i)
			}
		}
		for i, task := range c.ReduceTasks {
			if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
				c.ReduceTasks[i].State = Idle
				log.Printf("Reduce task %d timed out. Reassigning...\n", i)
			}
		}
		c.mu.Unlock()
	}
}

// Done checks if all tasks are done.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.AllTasksDone
}

// server starts the RPC server for worker communication.
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, err := net.Listen("tcp", sockname)
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	log.Println("Coordinator started at", sockname)
	go http.Serve(l, nil)
}

// MakeCoordinator initializes a Coordinator with map and reduce tasks.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:     make([]Task, len(files)),
		ReduceTasks:  make([]Task, nReduce),
		NReduce:      nReduce,
		NMap:         len(files),
		WorkerAddrs:  make(map[int]string),
		NextWorkerID: 0,
	}

	for i, file := range files {
		c.MapTasks[i] = Task{
			FileName: file,
			TaskType: "map",
			State:    Idle,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			TaskType: "reduce",
			State:    Idle,
		}
	}

	go c.monitorTasks()
	c.server()
	return &c
}

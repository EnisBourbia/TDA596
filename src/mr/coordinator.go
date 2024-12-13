package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// TaskState defines the possible states of a task: Idle, InProgress, or Completed.
type TaskState int

const (
	Idle       TaskState = iota // Task is waiting to be assigned.
	InProgress                  // Task is currently being executed.
	Completed                   // Task has been completed.
)

// Task represents a single unit of work (either map or reduce).
type Task struct {
	FileName   string    // Name of the file for a map task (empty for reduce tasks).
	TaskType   string    // Type of the task ("map" or "reduce").
	State      TaskState // Current state of the task (Idle, InProgress, or Completed).
	WorkerID   int       // ID of the worker currently assigned to the task.
	StartTime  time.Time // Timestamp of when the task was assigned (used for timeout detection).
	WorkerAddr string    // Address of the worker executing this task.
}

// Coordinator manages the MapReduce process, distributing tasks to workers.
type Coordinator struct {
	MapTasks     []Task     // List of map tasks to be executed.
	ReduceTasks  []Task     // List of reduce tasks to be executed.
	NReduce      int        // Number of reduce tasks.
	NMap         int        // Number of map tasks (or input files).
	AllTasksDone bool       // Flag indicating if all tasks are completed.
	mu           sync.Mutex // Mutex for synchronizing access to shared resources.

	WorkerAddrs  map[int]string // Map of WorkerID to their network address.
	NextWorkerID int            // Counter for assigning unique WorkerIDs.
}

// AssignTask provides a worker with a new task to execute.
// It handles both map and reduce tasks, checking their availability and state.
func (c *Coordinator) AssignTask(req *TaskRequest, res *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If all tasks are completed, notify the worker.
	if c.AllTasksDone {
		res.AllDone = true
		return nil
	}

	// Update the worker's address in the WorkerAddrs map.
	c.WorkerAddrs[req.WorkerID] = req.Address

	// Assign a map task if any are still idle.
	for i, task := range c.MapTasks {
		if task.State == Idle {
			content, err := ioutil.ReadFile(task.FileName) // Read the input file for the map task.
			if err != nil {
				log.Printf("Failed to read input file %v: %v", task.FileName, err)
				continue // Skip the task if the file cannot be read.
			}

			// Update the task state and metadata.
			c.MapTasks[i].State = InProgress
			c.MapTasks[i].WorkerID = req.WorkerID
			c.MapTasks[i].StartTime = time.Now()
			c.MapTasks[i].WorkerAddr = req.Address

			// Prepare the task response for the worker.
			res.TaskType = "map"
			res.TaskID = i
			res.FileName = task.FileName
			res.InputData = string(content) // Provide the file content for the task.
			res.NReduce = c.NReduce
			res.WorkerID = req.WorkerID
			return nil
		}
	}

	// Check if all map tasks are completed before assigning reduce tasks.
	mapDone := true
	for _, task := range c.MapTasks {
		if task.State != Completed {
			mapDone = false
			break
		}
	}
	if mapDone {
		// Assign a reduce task if any are still idle.
		for i, task := range c.ReduceTasks {
			if task.State == Idle {
				c.ReduceTasks[i].State = InProgress
				c.ReduceTasks[i].WorkerID = req.WorkerID
				c.ReduceTasks[i].StartTime = time.Now()

				// Collect addresses of all map workers for fetching intermediate data.
				mapWorkerAddrs := make(map[int]string)
				for idx, mapTask := range c.MapTasks {
					mapWorkerAddrs[idx] = mapTask.WorkerAddr
				}

				// Prepare the reduce task response for the worker.
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

	// If no tasks are ready, inform the worker to wait.
	res.TaskType = "wait"
	return nil
}

// MarkTaskCompleted is called by workers to notify the coordinator of task completion.
func (c *Coordinator) MarkTaskCompleted(args *TaskCompleteArgs, _ *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		// Mark the specified map task as completed.
		if c.MapTasks[args.TaskID].WorkerID == args.WorkerID {
			c.MapTasks[args.TaskID].State = Completed
		}
	} else if args.TaskType == "reduce" {
		// Mark the specified reduce task as completed.
		if c.ReduceTasks[args.TaskID].WorkerID == args.WorkerID {
			c.ReduceTasks[args.TaskID].State = Completed
		}
	}

	// Check if all tasks are now done.
	c.checkAllTasksDone()
	return nil
}

// RegisterWorker registers a new worker with the coordinator and assigns it a unique WorkerID.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Assign a unique WorkerID and store the worker's address.
	c.NextWorkerID++
	workerID := c.NextWorkerID
	c.WorkerAddrs[workerID] = args.Address
	reply.WorkerID = workerID
	return nil
}

// checkAllTasksDone verifies if all map and reduce tasks are completed.
// If so, it sets the AllTasksDone flag to true.
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

// monitorTasks continuously checks for tasks that have timed out
// and reassigns them by resetting their state to Idle.
func (c *Coordinator) monitorTasks() {
	for {
		time.Sleep(1000 * time.Millisecond)
		c.mu.Lock()
		for i, task := range c.MapTasks {
			if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
				// Reset the task to Idle if it times out.
				c.MapTasks[i].State = Idle
				log.Printf("Map task %d timed out. Reassigning...\n", i)
			}
		}
		for i, task := range c.ReduceTasks {
			if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
				// Reset the task to Idle if it times out.
				c.ReduceTasks[i].State = Idle
				log.Printf("Reduce task %d timed out. Reassigning...\n", i)
			}
		}
		c.mu.Unlock()
	}
}

// Done is called periodically by the main program to check if all tasks are completed.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.AllTasksDone
}

// server starts an RPC server to handle worker requests.
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

// MakeCoordinator initializes a Coordinator with the given map input files and reduce tasks.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:     make([]Task, len(files)),
		ReduceTasks:  make([]Task, nReduce),
		NReduce:      nReduce,
		NMap:         len(files),
		WorkerAddrs:  make(map[int]string),
		NextWorkerID: 0,
	}

	// Initialize map tasks for each input file.
	for i, file := range files {
		c.MapTasks[i] = Task{
			FileName: file,
			TaskType: "map",
			State:    Idle,
		}
	}

	// Initialize reduce tasks.
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			TaskType: "reduce",
			State:    Idle,
		}
	}

	// Start the task monitoring goroutine.
	go c.monitorTasks()

	// Start the RPC server for worker communication.
	c.server()
	return &c
}

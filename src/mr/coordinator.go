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

// TaskStatus represents the possible states of a task.
type TaskStatus int

const (
	Idle       TaskStatus = iota // Task is not yet assigned to a worker.
	InProgress                   // Task is currently being executed by a worker.
	Completed                    // Task has been successfully completed.
)

// TaskInfo holds information about a map or reduce task.
type TaskInfo struct {
	FileName  string     // Input file for the task (used for map tasks).
	Status    TaskStatus // Current status of the task (Idle, InProgress, or Completed).
	WorkerID  int        // The ID of the worker currently handling this task.
	StartTime time.Time  // The timestamp when the task was assigned (used to detect timeouts).
}

// Coordinator orchestrates the distribution of tasks to workers.
type Coordinator struct {
	Mutex        sync.Mutex // Protects shared resources like tasks from concurrent access.
	MapTasks     []TaskInfo // List of all map tasks to be executed.
	ReduceTasks  []TaskInfo // List of all reduce tasks to be executed.
	NReduce      int        // Number of reduce tasks.
	NMap         int        // Number of map tasks (or input files).
	AllTasksDone bool       // Indicates if all map and reduce tasks have been completed.
}

// RequestTask assigns a task to a worker that requests it.
// The coordinator decides whether to assign a map, reduce, or no task based on task states.
func (c *Coordinator) RequestTask(req *TaskRequest, res *TaskReply) error {
	c.Mutex.Lock() // Lock to ensure thread-safe access to shared task data.
	defer c.Mutex.Unlock()

	// If all tasks are completed, inform the worker.
	if c.AllTasksDone {
		res.Completed = true
		return nil
	}

	// Assign an idle map task to the requesting worker, if available.
	for i := range c.MapTasks {
		task := &c.MapTasks[i]
		if task.Status == Idle {
			// Update the task's status and metadata.
			task.Status = InProgress
			task.WorkerID = req.WorkerID
			task.StartTime = time.Now()

			// Populate the task details in the response.
			res.Type = "Map"
			res.TaskID = i
			res.FileName = task.FileName
			res.NReduce = c.NReduce
			return nil
		}
	}

	// If all map tasks are completed, assign an idle reduce task.
	if c.allMapTasksCompleted() {
		for i := range c.ReduceTasks {
			task := &c.ReduceTasks[i]
			if task.Status == Idle {
				// Update the task's status and metadata.
				task.Status = InProgress
				task.WorkerID = req.WorkerID
				task.StartTime = time.Now()

				// Populate the task details in the response.
				res.Type = "Reduce"
				res.TaskID = i
				res.NMap = c.NMap
				return nil
			}
		}
	}

	// If no tasks are available, tell the worker to wait.
	res.Type = "Wait"
	return nil
}

// ReportCompletion is called by workers to notify the coordinator that a task is complete.
func (c *Coordinator) ReportCompletion(args *TaskCompletion, reply *EmptyReply) error {
	c.Mutex.Lock() // Lock to ensure thread-safe updates to task states.
	defer c.Mutex.Unlock()

	if args.Type == "Map" {
		// Validate and mark the map task as completed.
		task := &c.MapTasks[args.TaskID]
		if task.WorkerID == args.WorkerID && task.Status == InProgress {
			task.Status = Completed
		}
	} else if args.Type == "Reduce" {
		// Validate and mark the reduce task as completed.
		task := &c.ReduceTasks[args.TaskID]
		if task.WorkerID == args.WorkerID && task.Status == InProgress {
			task.Status = Completed
		}
	}

	// Check if all tasks are now completed.
	c.checkCompletion()
	return nil
}

// checkCompletion checks whether all map and reduce tasks have been completed.
// If so, it sets the AllTasksDone flag to true.
func (c *Coordinator) checkCompletion() {
	if c.allMapTasksCompleted() && c.allReduceTasksCompleted() {
		c.AllTasksDone = true
	}
}

// allMapTasksCompleted checks if all map tasks are in the Completed state.
func (c *Coordinator) allMapTasksCompleted() bool {
	for _, task := range c.MapTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

// allReduceTasksCompleted checks if all reduce tasks are in the Completed state.
func (c *Coordinator) allReduceTasksCompleted() bool {
	for _, task := range c.ReduceTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

// monitorTasks continuously checks for tasks that are taking too long to complete (timeout).
// Timed-out tasks are reset to Idle so they can be reassigned.
func (c *Coordinator) monitorTasks() {
	for {
		time.Sleep(time.Second) // Periodically check tasks every second.
		c.Mutex.Lock()
		if c.AllTasksDone {
			// Exit the monitoring loop if all tasks are completed.
			c.Mutex.Unlock()
			break
		}
		currentTime := time.Now()
		// Reassign timed-out map tasks.
		for i := range c.MapTasks {
			task := &c.MapTasks[i]
			if task.Status == InProgress && currentTime.Sub(task.StartTime) > 10*time.Second {
				task.Status = Idle
			}
		}
		// Reassign timed-out reduce tasks.
		for i := range c.ReduceTasks {
			task := &c.ReduceTasks[i]
			if task.Status == InProgress && currentTime.Sub(task.StartTime) > 10*time.Second {
				task.Status = Idle
			}
		}
		c.Mutex.Unlock()
	}
}

// Done is periodically called by the main program to check if all tasks are finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.AllTasksDone
}

// server starts the coordinator's RPC server to handle worker requests.
func (c *Coordinator) server() {
	rpc.Register(c)               // Register the Coordinator type for RPC.
	rpc.HandleHTTP()              // Set up HTTP handlers for RPC communication.
	sockname := coordinatorSock() // Generate a unique socket name for the coordinator.
	os.Remove(sockname)           // Remove any pre-existing socket with the same name.
	l, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("Coordinator listen error:", err)
	}
	go http.Serve(l, nil) // Start the server in a new goroutine.
}

// MakeCoordinator initializes a Coordinator and sets up map and reduce tasks.
// It also starts the task monitoring goroutine and the RPC server.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:     make([]TaskInfo, len(files)), // Create map tasks for each input file.
		ReduceTasks:  make([]TaskInfo, nReduce),    // Create reduce tasks.
		NReduce:      nReduce,
		NMap:         len(files),
		AllTasksDone: false,
	}

	// Initialize the map tasks with their associated input files.
	for i, file := range files {
		c.MapTasks[i] = TaskInfo{
			FileName: file,
			Status:   Idle,
		}
	}

	// Initialize the reduce tasks.
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = TaskInfo{
			Status: Idle,
		}
	}

	// Start a goroutine to monitor and reassign timed-out tasks.
	go c.monitorTasks()

	// Start the coordinator's RPC server to handle worker requests.
	c.server()
	return &c
}

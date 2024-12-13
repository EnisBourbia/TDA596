package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue represents a single key-value pair, used in both the map and reduce phases.
type KeyValue struct {
	Key   string
	Value string
}

// IP struct is used to parse the worker's public IP from the external API response.
type IP struct {
	Query string
}

// WorkerInstance represents a single worker process.
type WorkerInstance struct {
	logger             *log.Logger // Logger for debugging and logging worker activity
	address            string      // Address of the worker's RPC server
	listener           net.Listener
	WorkerID           int    // Unique ID assigned by the coordinator
	CoordinatorAddress string // Address of the coordinator's RPC server
}

// ByKey is a type alias to allow sorting a slice of KeyValue by their keys.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// ihash hashes a string key and returns a non-negative integer.
// This is used to partition intermediate data into buckets for reduce tasks.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is the entry point for a worker process. It initializes the worker, registers with the coordinator,
// and continuously requests and processes tasks until all tasks are completed.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Address of the coordinator RPC server (replace with actual coordinator address)
	var coordinatorAddress string = "13.60.16.194:1234"

	// Initialize logger
	logger := log.New(os.Stdout, "[Worker] ", log.LstdFlags)
	worker := WorkerInstance{
		logger:             logger,
		CoordinatorAddress: coordinatorAddress,
	}

	// Start the worker's RPC server
	worker.startRPCServer()
	defer worker.listener.Close() // Ensure the RPC server is closed when the worker exits

	// Register this worker with the coordinator
	worker.RegisterWithCoordinator()

	logger.Println("Starting worker...")

	// Main loop: Continuously request tasks and process them
	for {
		task := worker.RequestTask()

		if task.AllDone {
			// Exit if all tasks are completed
			logger.Println("All tasks are completed. Exiting worker.")
			break
		}

		// Process the task based on its type (map or reduce)
		if task.TaskType == "map" {
			logger.Printf("Starting map task: ID=%d, File=%s\n", task.TaskID, task.FileName)
			worker.PerformMapTask(task, mapf)
			logger.Printf("Completed map task: ID=%d\n", task.TaskID)
		} else if task.TaskType == "reduce" {
			logger.Printf("Starting reduce task: ID=%d\n", task.TaskID)
			worker.PerformReduceTask(task, reducef)
			logger.Printf("Completed reduce task: ID=%d\n", task.TaskID)
		} else if task.TaskType == "wait" {
			// No tasks are available; wait before requesting again
			logger.Println("No tasks available. Waiting...")
			time.Sleep(time.Second)
			continue
		} else {
			// Unknown task type; exit gracefully
			logger.Println("Unknown task type. Exiting.")
			break
		}

		// Report task completion to the coordinator
		worker.ReportTaskCompletion(task.TaskType, task.TaskID)
		logger.Printf("Reported task completion: Type=%s, ID=%d\n", task.TaskType, task.TaskID)
	}
}

// startRPCServer starts the worker's RPC server and obtains its public IP for communication.
func (w *WorkerInstance) startRPCServer() {
	rpc.Register(w) // Register the worker instance for RPC
	rpc.HandleHTTP()

	// Obtain the worker's public IP address using an external API
	req, err := http.Get("http://ip-api.com/json/")
	if err != nil {
		log.Fatalf("Could not get IP: %v", err)
	}
	defer req.Body.Close()

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Fatalf("Could not read IP response: %v", err)
	}

	var ip IP
	json.Unmarshal(body, &ip)
	publicIP := ip.Query // Extract the public IP

	// Start an RPC server on the public IP with an ephemeral port
	listener, err := net.Listen("tcp", publicIP+":0")
	if err != nil {
		log.Fatal("Worker failed to listen:", err)
	}

	w.address = listener.Addr().String() // Store the worker's address
	w.listener = listener
	go http.Serve(listener, nil) // Serve RPC requests in a separate goroutine
	w.logger.Println("Worker RPC server listening at", w.address)
}

// RegisterWithCoordinator registers the worker with the coordinator and obtains a unique WorkerID.
func (w *WorkerInstance) RegisterWithCoordinator() {
	args := RegisterWorkerArgs{Address: w.address}
	var reply RegisterWorkerReply
	ok := call(w.CoordinatorAddress, "Coordinator.RegisterWorker", &args, &reply)
	if !ok {
		w.logger.Fatal("Failed to register with coordinator.")
	}
	w.WorkerID = reply.WorkerID // Store the assigned WorkerID
	w.logger.Printf("Registered with coordinator as WorkerID %d", w.WorkerID)
}

// RequestTask asks the coordinator for a task.
func (w *WorkerInstance) RequestTask() TaskResponse {
	args := TaskRequest{WorkerID: w.WorkerID, Address: w.address}
	reply := TaskResponse{}

	w.logger.Println("Requesting a task from the coordinator...")
	ok := call(w.CoordinatorAddress, "Coordinator.AssignTask", &args, &reply)
	if !ok {
		w.logger.Println("Failed to contact coordinator. Exiting.")
		os.Exit(0) // Coordinator is unreachable; exit gracefully
	}
	w.logger.Printf("Received task response: %+v\n", reply)
	return reply
}

// PerformMapTask executes a map task assigned by the coordinator.
func (w *WorkerInstance) PerformMapTask(task TaskResponse, mapf func(string, string) []KeyValue) {
	content := task.InputData // Get input data for the map task
	if content == "" {
		w.logger.Fatalf("Received empty input data for file %v", task.FileName)
	}

	// Apply the user-defined map function to generate intermediate key-value pairs
	kvs := mapf(task.FileName, content)

	// Partition the key-value pairs into buckets based on their hash
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// Write each bucket to a separate intermediate file
	for i, kvs := range buckets {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, i) // File format: mr-taskID-bucketID
		file, err := os.Create(filename)
		if err != nil {
			w.logger.Fatalf("Failed to create output file %v: %v", filename, err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				w.logger.Fatalf("Failed to write to output file %v: %v", filename, err)
			}
		}
		file.Close()
		w.logger.Printf("Wrote intermediate results to %s\n", filename)
	}
}

// PerformReduceTask executes a reduce task assigned by the coordinator.
func (w *WorkerInstance) PerformReduceTask(task TaskResponse, reducef func(string, []string) string) {
	var kvs []KeyValue // Collect all key-value pairs from intermediate files

	// Fetch intermediate data from all map workers for this reduce task
	for mapTaskID, workerAddress := range task.MapWorkerAddrs {
		args := GetIntermediateDataArgs{
			MapTaskID:    mapTaskID,
			ReduceTaskID: task.TaskID,
		}
		var reply GetIntermediateDataReply
		ok := call(workerAddress, "WorkerInstance.GetIntermediateData", &args, &reply)
		if !ok {
			w.logger.Printf("Failed to get data from worker %s\n", workerAddress)
			continue // Skip failed workers
		}
		kvs = append(kvs, reply.Data...)
	}

	// Sort the key-value pairs by key to group them for reduction
	sort.Sort(ByKey(kvs))

	// Create the output file for the reduce task
	outputFile := fmt.Sprintf("mr-out-%d", task.TaskID)
	file, err := os.Create(outputFile)
	if err != nil {
		w.logger.Fatalf("Failed to create output file %v: %v", outputFile, err)
	}

	// Perform the reduction by grouping values for each key and applying reducef
	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	file.Close()
	w.logger.Printf("Wrote reduce results to %s\n", outputFile)
}

// ReportTaskCompletion informs the coordinator that a task is complete.
func (w *WorkerInstance) ReportTaskCompletion(taskType string, taskID int) {
	args := TaskCompleteArgs{TaskType: taskType, TaskID: taskID, WorkerID: w.WorkerID}
	reply := struct{}{}

	w.logger.Printf("Reporting task completion: Type=%s, ID=%d\n", taskType, taskID)
	ok := call(w.CoordinatorAddress, "Coordinator.MarkTaskCompleted", &args, &reply)
	if !ok {
		w.logger.Println("Failed to report task completion to coordinator. Exiting.")
		os.Exit(0)
	}
}

// GetIntermediateData serves intermediate data to reduce workers.
func (w *WorkerInstance) GetIntermediateData(args *GetIntermediateDataArgs, reply *GetIntermediateDataReply) error {
	// Open the file corresponding to the requested map task and reduce task
	filename := fmt.Sprintf("mr-%d-%d", args.MapTaskID, args.ReduceTaskID)
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Decode the JSON-encoded key-value pairs from the file
	dec := json.NewDecoder(file)
	var kvs []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	reply.Data = kvs // Return the key-value pairs to the requesting worker
	return nil
}

// call performs an RPC call to the given address and method, with the specified arguments and reply.
func call(address string, rpcname string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println("Dialing failed:", err)
		return false
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	log.Println("RPC call failed:", err)
	return false
}

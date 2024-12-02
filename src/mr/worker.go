package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type WorkerInstance struct {
	logger             *log.Logger
	address            string
	listener           net.Listener
	WorkerID           int
	CoordinatorAddress string

	// You can add more fields if needed
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is the entry point for a worker process.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var coordinatorAddress string = "13.60.16.194:1234"
	/*flag.StringVar(&coordinatorAddress, "coordinator", "", "Coordinator address")
	flag.Parse()

	if coordinatorAddress == "" {
		log.Fatal("Coordinator address must be provided with -coordinator flag")
	}
	*/

	logger := log.New(os.Stdout, "[Worker] ", log.LstdFlags)
	worker := WorkerInstance{
		logger:             logger,
		CoordinatorAddress: coordinatorAddress,
	}

	worker.startRPCServer()
	defer worker.listener.Close()

	worker.RegisterWithCoordinator()

	logger.Println("Starting worker...")

	for {
		task := worker.RequestTask()
		if task.AllDone {
			logger.Println("All tasks are completed. Exiting worker.")
			break
		}

		if task.TaskType == "map" {
			logger.Printf("Starting map task: ID=%d, File=%s\n", task.TaskID, task.FileName)
			worker.PerformMapTask(task, mapf)
			logger.Printf("Completed map task: ID=%d\n", task.TaskID)
		} else if task.TaskType == "reduce" {
			logger.Printf("Starting reduce task: ID=%d\n", task.TaskID)
			worker.PerformReduceTask(task, reducef)
			logger.Printf("Completed reduce task: ID=%d\n", task.TaskID)
		} else if task.TaskType == "wait" {
			logger.Println("No tasks available. Waiting...")
			time.Sleep(time.Second)
			continue
		} else {
			logger.Println("Unknown task type. Exiting.")
			break
		}

		worker.ReportTaskCompletion(task.TaskType, task.TaskID)
		logger.Printf("Reported task completion: Type=%s, ID=%d\n", task.TaskType, task.TaskID)
	}
}

func (w *WorkerInstance) startRPCServer() {
	rpc.Register(w)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":0") // OS assigns an available port
	if err != nil {
		log.Fatal("Worker failed to listen:", err)
	}
	w.address = listener.Addr().String()
	w.listener = listener
	go http.Serve(listener, nil)
	w.logger.Println("Worker RPC server listening at", w.address)
}

func (w *WorkerInstance) RegisterWithCoordinator() {
	args := RegisterWorkerArgs{Address: w.address}
	var reply RegisterWorkerReply
	ok := call(w.CoordinatorAddress, "Coordinator.RegisterWorker", &args, &reply)
	if !ok {
		w.logger.Fatal("Failed to register with coordinator.")
	}
	w.WorkerID = reply.WorkerID
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
	content := task.InputData
	if content == "" {
		w.logger.Fatalf("Received empty input data for file %v", task.FileName)
	}

	kvs := mapf(task.FileName, content)
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	for i, kvs := range buckets {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
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
	var kvs []KeyValue
	for mapTaskID, workerAddress := range task.MapWorkerAddrs {
		args := GetIntermediateDataArgs{
			MapTaskID:    mapTaskID,
			ReduceTaskID: task.TaskID,
		}
		var reply GetIntermediateDataReply
		ok := call(workerAddress, "WorkerInstance.GetIntermediateData", &args, &reply)
		if !ok {
			w.logger.Printf("Failed to get data from worker %s\n", workerAddress)
			continue // Handle failure by skipping this map task
		}
		kvs = append(kvs, reply.Data...)
	}

	sort.Sort(ByKey(kvs))
	outputFile := fmt.Sprintf("mr-out-%d", task.TaskID)
	file, err := os.Create(outputFile)
	if err != nil {
		w.logger.Fatalf("Failed to create output file %v: %v", outputFile, err)
	}

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
	filename := fmt.Sprintf("mr-%d-%d", args.MapTaskID, args.ReduceTaskID)
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	var kvs []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	reply.Data = kvs
	return nil
}

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

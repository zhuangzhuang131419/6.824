package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	workID := RegisterWorker()

	for  {
		task := RequestTask(workID)
		if !task.Alive {
			fmt.Printf("Worker get task is not alive, %d\n", workID)
			return
		}
		DoTask(task, workID, mapf, reducef)
	}



	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func RegisterWorker() int {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	// fmt.Printf("Worker %v Register\n", reply.WorkID)
	if ok := call("Master.RegisterWorker", &args, &reply); !ok {
		log.Fatal("Register Worker Error")
	}
	// fmt.Printf("Worker %v Register Successfully\n", reply.WorkID)
	return reply.WorkID
}

func RequestTask(workID int) *Task {
	args := RequestTaskArgs{
		WorkID: workID,
	}
	reply := RequestTaskReply{}
	if ok := call("Master.RequestTask", &args, &reply); !ok {
		log.Fatal("Request Task Error")
	}
	// fmt.Printf("Worker %v Request Task %v Successfully\n", workID, reply.Task.TaskID)
	return reply.Task
}

func ReportTask(done bool, task *Task, workID int) {
	args := ReportTaskArgs{
		Done:   done,
		TaskID: task.TaskID,
		Phase:  task.Phase,
		WorkID: workID,
	}
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		log.Fatal("Report Task Error")
	}
	// fmt.Printf("Worker %v Report Task %v Successfully\n", workID, task.TaskID)
	return
}

func DoTask(task *Task, workID int, mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {
	// fmt.Printf("Worker %v, Do %v Task\n", workID, task.Phase)
	if task.Phase == MapPhase {
		doMapTask(task, workID, mapFunction)
	} else if task.Phase == ReducePhase {
		doReduceTask(task, workID, reduceFunction)
	} else {
		panic(fmt.Sprintf("Task phase err: %v", task.Phase))
	}
}

func doMapTask(task *Task, workID int, mapFunction func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		ReportTask(false, task, workID)
		log.Fatalf("ReadFile Error: %v", err)
	}
	kvs := mapFunction(task.FileName, string(content))
	intermediates := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.NReduce
		intermediates[idx] = append(intermediates[idx], kv)
	}

	for i, intermediate := range intermediates {
		fileName := fmt.Sprintf("mr-%v-%v", task.TaskID, i) // 由第i个reduce worker处理的第taskID个任务
		f, err := os.Create(fileName)
		if err != nil {
			ReportTask(false, task, workID)
			log.Fatalf("CreateFile Error: %v", err)
		}

		encoder := json.NewEncoder(f)
		for _, value := range intermediate {
			if err := encoder.Encode(value); err != nil {
				ReportTask(false, task, workID)
				log.Fatalf("Encode Error: %v", err)
			}
		}
		if err := f.Close(); err != nil {
			ReportTask(false, task, workID)
			log.Fatalf("FileClose Error: %v", err)
		}
	}
	ReportTask(true, task, workID)
}

func doReduceTask(task *Task, workID int, reduceFunction func(string, []string) string) {
	intermediates := make(map[string][]string)
	for i := 0; i < task.NMaps; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", i, task.TaskID)
		file, err := os.Open(fileName)
		if err != nil {
			ReportTask(false, task, workID)
			log.Fatalf("Open file Error: %v", err)
		}
		decoder := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := intermediates[kv.Key]; !ok {
				intermediates[kv.Key] = make([]string, 0, 100)
			}
			intermediates[kv.Key] = append(intermediates[kv.Key], kv.Value)
		}
	}
	result := make([]string, 0, 100)
	for k, v := range intermediates {
		result = append(result, fmt.Sprintf("%v %v\n", k, reduceFunction(k, v)))
	}

	if err := ioutil.WriteFile(fmt.Sprintf("mr-out-%d", task.TaskID), []byte(strings.Join(result, "")), 0600); err != nil {
		ReportTask(false, task, workID)
		return
	}
	ReportTask(true, task, workID)

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

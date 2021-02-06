package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.

	taskChan   chan Task // 任务队列
	filesName  []string
	nReduce    int
	taskPhase  TaskPhase    //
	taskStatus []TaskStatus // 任务状态检测
	workerID   int
	mutex      sync.Mutex
	done       bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.done
}

func (m *Master) InitMapTask() {
	m.taskPhase = MapPhase
	m.taskStatus = make([]TaskStatus, len(m.filesName))
	// fmt.Println("Map Task Init...")
}

func (m *Master) InitReduceTask() {
	m.taskPhase = ReducePhase
	m.taskStatus = make([]TaskStatus, m.nReduce)
	// fmt.Println("Reduce Task Init...")
}

func (m *Master) AddTask(taskID int) {
	if taskID >= len(m.taskStatus) {
		log.Fatalf("UnValid Task ID %v\n", taskID)
		return
	}
	// log.Printf("Add Task %v\n", taskID)
	m.taskStatus[taskID].Status = TaskStatusQueue
	task := Task{
		NReduce: m.nReduce,
		NMaps:   len(m.filesName),
		TaskID:  taskID,
		Phase:   m.taskPhase,
		Alive:   true,
	}

	if m.taskPhase == MapPhase {
		task.FileName = m.filesName[taskID]
	}

	m.taskChan <- task
}

func (m *Master) registerTask(workID int, task *Task) {
	// fmt.Println("Register Task Lock")
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.taskStatus[task.TaskID].Status = TaskStatusRunning
	m.taskStatus[task.TaskID].StartTime = time.Now()
	m.taskStatus[task.TaskID].WorkID = workID
	// fmt.Println("Register Task UnLock")
}

func (m *Master) checkBreak(taskID int) {
	timeGap := time.Now().Sub(m.taskStatus[taskID].StartTime)
	if timeGap > 5 * time.Second {
		m.AddTask(taskID)
	}
}

func (m *Master) Schedule() {
	// 定期执行，监测任务状态和流程
	allFinish := true
	//fmt.Println("Schedule Lock")
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.done {
		return
	}

	for i, task := range m.taskStatus {
		switch task.Status {
		case TaskStatusReady:
			allFinish = false
			m.AddTask(i)
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			m.checkBreak(i)
		case TaskStatusFinished:
		case TaskStatusErr:
			allFinish = false
			m.AddTask(i)
		default:
			panic("tasks status schedule error...")
		}
	}

	if allFinish {
		if m.taskPhase == MapPhase {
			m.InitReduceTask()
		} else {
			m.done = true
		}
	}
	// fmt.Println("Schedule UnLock")
}

func (m *Master) Tick() {
	for !m.Done() {
		// fmt.Println("loop...")
		go m.Schedule()
		time.Sleep(time.Second)
	}
	fmt.Println("Finish Task!!")
}

// exposed func to worker here
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	// fmt.Println("Register Worker Lock")
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.workerID += 1
	reply.WorkID = m.workerID
	// fmt.Println("Register Worker UnLock")
	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// fmt.Println("Request Task...")
	task := <-m.taskChan
	reply.Task = &task
	m.registerTask(args.WorkID, &task)
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	// fmt.Println("Report Task Lock")
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if args.Done {
		m.taskStatus[args.TaskID].Status = TaskStatusFinished
	} else {
		m.taskStatus[args.TaskID].Status = TaskStatusErr
	}
	go m.Schedule()
	// fmt.Println("Report Task UnLock")
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		taskChan:   make(chan Task, nReduce),
		filesName:  files,
		nReduce:    nReduce,
		mutex:      sync.Mutex{},
	}

	m.InitMapTask()
	go m.Tick()

	// Your code here.

	m.server()
	// fmt.Println("master init...")
	return &m
}

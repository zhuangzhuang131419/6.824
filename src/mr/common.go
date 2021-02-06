package mr

import "time"

type TaskPhase int
type Status int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

const (
	TaskStatusReady    Status = 0
	TaskStatusQueue    Status = 1
	TaskStatusRunning  Status = 2
	TaskStatusFinished Status = 3
	TaskStatusErr      Status = 4
)

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	TaskID   int
	Phase    TaskPhase
	Alive    bool
}

type TaskStatus struct {
	TaskID    int
	Status    Status
	StartTime time.Time
	WorkID    int
}

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// TaskType describes the kind of work the coordinator hands to a worker.
type TaskType int

const (
	// MapTask tells the worker to map Task.File into Task.NReduce buckets.
	MapTask TaskType = iota
	// ReduceTask tells the worker to reduce bucket Task.Id.
	ReduceTask
	// WaitTask means no work is available right now; ask again shortly.
	WaitTask
	// ExitTask means the job is finished; the worker should stop.
	ExitTask
)

// Task is a unit of work handed out by the coordinator.
type Task struct {
	Type    TaskType
	Id      int    // map-task index, or reduce-task index
	File    string // MapTask: input file to process
	NReduce int    // MapTask: how many reduce buckets to partition into
	NMaps   int    // ReduceTask: how many map outputs to read
	Gen     int    // issue generation; stale reports are ignored
}

// GetTaskArgs and GetTaskReply are the payloads of the GetTask RPC.
type GetTaskArgs struct{}

type GetTaskReply struct {
	Task Task
}

// ReportTaskArgs is the payload of the ReportTask RPC.
type ReportTaskArgs struct {
	Type TaskType
	Id   int
	Gen  int
	Ok   bool // false means the task failed and must be re-run
}

type ReportTaskReply struct{}

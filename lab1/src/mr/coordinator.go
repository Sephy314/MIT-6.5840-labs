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

// taskTimeout is how long a task may run before the coordinator assumes its
// worker has crashed and hands the task out again.
const taskTimeout = 10 * time.Second

type taskState int

const (
	tsIdle taskState = iota
	tsRunning
	tsDone
)

type taskInfo struct {
	id    int // map-task index, or reduce-task index
	state taskState
	gen   int       // bumped on every issue; stale reports carry an old gen
	start time.Time // when the task was last handed out
}

// Coordinator schedules map and reduce tasks for a job.
//
// The map phase has one task per input file; the reduce phase has one task per
// bucket. Reduce tasks are only issued after every map task has finished. A
// task left running for longer than taskTimeout is re-issued (the worker is
// assumed to have crashed).
type Coordinator struct {
	mu       sync.Mutex
	files    []string
	nMap     int
	nReduce  int
	maps     []*taskInfo
	reduces  []*taskInfo
	mapDone  int
	redDone  int
	redStart bool // reduce phase has started
	allDone  bool // every map and reduce task is done
}

// GetTask hands out the next unit of work: a map task while the map phase is
// running, a reduce task once all maps are done, ExitTask when the whole job
// is done, or WaitTask when there is nothing idle right now.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Map phase: hand out idle map tasks until every one is done.
	if !c.redStart {
		if c.mapDone == c.nMap {
			c.redStart = true
		} else {
			for _, m := range c.maps {
				if m.state == tsIdle {
					c.start(m)
					reply.Task = Task{Type: MapTask, Id: m.id, File: c.files[m.id], NReduce: c.nReduce, Gen: m.gen}
					return nil
				}
			}
			reply.Task = Task{Type: WaitTask} // maps still running
			return nil
		}
	}

	// Reduce phase.
	if c.redDone == c.nReduce {
		c.allDone = true
		reply.Task = Task{Type: ExitTask}
		return nil
	}
	for _, r := range c.reduces {
		if r.state == tsIdle {
			c.start(r)
			reply.Task = Task{Type: ReduceTask, Id: r.id, NMaps: c.nMap, Gen: r.gen}
			return nil
		}
	}
	reply.Task = Task{Type: WaitTask} // reduces still running
	return nil
}

// start marks t as running and hands it out with a fresh generation.
func (c *Coordinator) start(t *taskInfo) {
	t.state = tsRunning
	t.gen++
	t.start = time.Now()
}

// ReportTask records the outcome of a task. Failed tasks are made runnable
// again. A report whose generation does not match the current issue is stale
// (it can only arrive after the task was re-issued) and is ignored.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var t *taskInfo
	isMap := false
	switch args.Type {
	case MapTask:
		if args.Id >= 0 && args.Id < len(c.maps) {
			t = c.maps[args.Id]
			isMap = true
		}
	case ReduceTask:
		if args.Id >= 0 && args.Id < len(c.reduces) {
			t = c.reduces[args.Id]
		}
	}
	if t == nil || t.state != tsRunning || t.gen != args.Gen {
		return nil // stale or spurious report
	}
	if !args.Ok {
		t.state = tsIdle // make the task runnable again elsewhere
		return nil
	}

	t.state = tsDone
	if isMap {
		c.mapDone++
	} else {
		c.redDone++
		if c.redDone == c.nReduce {
			c.allDone = true
		}
	}
	return nil
}

// Done reports whether every map and reduce task has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allDone
}

// reaper periodically re-issues tasks whose workers have not reported within
// taskTimeout, recovering from worker crashes.
func (c *Coordinator) reaper() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		if c.allDone {
			c.mu.Unlock()
			return
		}
		now := time.Now()
		for _, m := range c.maps {
			if m.state == tsRunning && now.Sub(m.start) > taskTimeout {
				m.state = tsIdle
			}
		}
		for _, r := range c.reduces {
			if r.state == tsRunning && now.Sub(r.start) > taskTimeout {
				r.state = tsIdle
			}
		}
		c.mu.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		files:   files,
		nMap:    len(files),
		nReduce: nReduce,
		maps:    make([]*taskInfo, len(files)),
		reduces: make([]*taskInfo, nReduce),
	}
	for i := range c.maps {
		c.maps[i] = &taskInfo{id: i}
	}
	for i := range c.reduces {
		c.reduces[i] = &taskInfo{id: i}
	}

	go c.reaper()
	c.server(sockname)
	return c
}

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// waitInterval is how long a worker waits before asking for work again when
// there is none right now.
const waitInterval = 250 * time.Millisecond

// intermediateName is the file that map task x writes for reduce bucket y.
func intermediateName(x, y int) string {
	return fmt.Sprintf("mr-%d-%d", x, y)
}

// outName is the final output file that reduce task y writes.
func outName(y int) string {
	return fmt.Sprintf("mr-out-%d", y)
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		var reply GetTaskReply
		if !call("Coordinator.GetTask", &GetTaskArgs{}, &reply) {
			// The coordinator is gone (the job is done); stop.
			return
		}

		task := reply.Task
		var err error
		switch task.Type {
		case MapTask:
			err = doMap(task, mapf)
		case ReduceTask:
			err = doReduce(task, reducef)
		case WaitTask:
			time.Sleep(waitInterval)
			continue
		case ExitTask:
			return
		default:
			log.Printf("worker: unknown task type %v", task.Type)
			return
		}
		if err != nil {
			log.Printf("worker: task failed (%v): %v", task, err)
		}
		report(task, err == nil)
	}
}

// doMap reads task.File, runs mapf over its contents, and partitions the
// emitted pairs across the task.NReduce intermediate files. Buckets are written
// to unique temp files and renamed only once every bucket has been written, so
// a crash never leaves a partially written mr-X-Y behind for a reducer.
func doMap(task Task, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(task.File)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	file.Close()
	if err != nil {
		return err
	}
	kva := mapf(task.File, string(content))

	ofiles := make([]*os.File, task.NReduce)
	encs := make([]*json.Encoder, task.NReduce)
	tmps := make([]string, task.NReduce)
	defer func() {
		for i, f := range ofiles {
			if f != nil {
				f.Close()
			}
			if tmps[i] != "" {
				os.Remove(tmps[i])
			}
		}
	}()

	for r := 0; r < task.NReduce; r++ {
		tmp, err := ioutil.TempFile(".", "mr-tmp-")
		if err != nil {
			return err
		}
		ofiles[r] = tmp
		tmps[r] = tmp.Name()
		encs[r] = json.NewEncoder(tmp)
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		if err := encs[r].Encode(&kv); err != nil {
			return err
		}
	}

	for r := 0; r < task.NReduce; r++ {
		if err := ofiles[r].Close(); err != nil {
			return err
		}
		ofiles[r] = nil
		if err := os.Rename(tmps[r], intermediateName(task.Id, r)); err != nil {
			return err
		}
		tmps[r] = "" // keep the renamed file
	}
	return nil
}

// doReduce reads every intermediate file mr-X-Y of reduce bucket task.Id,
// shuffles (sorts) the pairs by key, applies reducef to each key, and writes
// the final mr-out-Y. The output is committed with a temp-then-rename so a
// crash cannot leave a truncated mr-out-Y.
func doReduce(task Task, reducef func(string, []string) string) error {
	var kva []KeyValue
	for x := 0; x < task.NMaps; x++ {
		name := intermediateName(x, task.Id)
		f, err := os.Open(name)
		if err != nil {
			return fmt.Errorf("open %s: %w", name, err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				f.Close()
				return err
			}
			kva = append(kva, kv)
		}
		f.Close()
	}

	sort.Slice(kva, func(i, j int) bool { return kva[i].Key < kva[j].Key })

	tmp, err := ioutil.TempFile(".", "mr-out-tmp-")
	if err != nil {
		return err
	}
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		if _, err := fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return err
		}
		i = j
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), outName(task.Id))
}

// report tells the coordinator whether a task finished successfully.
func report(task Task, ok bool) {
	call("Coordinator.ReportTask",
		&ReportTaskArgs{Type: task.Type, Id: task.Id, Gen: task.Gen, Ok: ok},
		&ReportTaskReply{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

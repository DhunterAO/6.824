package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus uint8
const (
	Idle TaskStatus = iota
	InProcess
	Completed
)

type MapTask struct {
	taskStatus TaskStatus
	filename string
}

type ReduceTask struct {
	taskStatus TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	phase uint8
	mapTasks []MapTask
	reduceTasks []ReduceTask

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	completed := 0
	for idx, mapTask := range c.mapTasks {
		if mapTask.taskStatus != Idle {
			if mapTask.taskStatus == Completed {
				completed++
			}
			continue
		}
		c.mapTasks[idx].taskStatus = InProcess
		reply.TaskType = 0
		reply.TaskID = uint(idx)
		reply.NReduce = len(c.reduceTasks)
		reply.Filename = mapTask.filename
		// fmt.Printf("worker get task %v of type %v\n", reply.TaskID, reply.TaskType)
		// fmt.Printf("fileName %v\n", reply.Filename)
		return nil
	}

	if completed < len(c.mapTasks) {
		reply.TaskType = 2
		return nil
	}

	for idx, reduceTask := range c.reduceTasks {
		if reduceTask.taskStatus != Idle {
			continue
		}
		c.reduceTasks[idx].taskStatus = InProcess
		reply.TaskType = 1
		reply.NMap = len(c.mapTasks)
		reply.TaskID = uint(idx)
		return nil
	}

	reply.TaskType = 3
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	if args.TaskType == 0 {
		c.mapTasks[args.TaskID].taskStatus = Completed
	} else {
		c.reduceTasks[args.TaskID].taskStatus = Completed
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	for _, task := range c.mapTasks {
		if task.taskStatus != Completed {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if task.taskStatus != Completed {
			return false
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make([]MapTask, len(files))
	c.reduceTasks = make([]ReduceTask, nReduce)

	for idx, filename := range files {
		c.mapTasks[idx].taskStatus = Idle
		c.mapTasks[idx].filename = filename
	}

	for idx := 0; idx < nReduce; idx++ {
		c.reduceTasks[idx].taskStatus = Idle
	}
	// print("nMap: ", len(files), "\n")
	// print("nReduce: ", nReduce, "\n")
	c.server()
	return &c
}

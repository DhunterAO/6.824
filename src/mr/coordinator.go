package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
// import "fmt"
import "sync"

type TaskStatus uint8
const (
	Idle TaskStatus = iota
	InProcess
	Completed
)

type ServerStatus uint8

const (
	NoTask ServerStatus = iota
	WithTask
	Faulty
)

type Task struct {
	status TaskStatus
	serverID int
}

type MapTask struct {
	Task
	filename string
}

type ReduceTask struct {
	Task
}

type Server struct {
	serverID int
	taskID int
	taskType int
	status ServerStatus
	lastHeartbeat int64
}

type Coordinator struct {
	phase uint8
	mapTasks []MapTask
	reduceTasks []ReduceTask
	servers []Server

	mu sync.Mutex
}

func (c *Coordinator) RegisterServer(args *RegisterServerArgs, reply *RegisterServerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	serverID := len(c.servers)
	server := Server{
		serverID: serverID,
		taskID: 0,
		taskType: 0,
		status: NoTask,
		lastHeartbeat: time.Now().Unix(),
	}
	c.servers = append(c.servers, server)
	// fmt.Printf("Register server %d\n", serverID)
	reply.ServerID = serverID
	return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	// fmt.Printf("Heartbeat from server %d\n", args.ServerID)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.servers[args.ServerID].lastHeartbeat = time.Now().Unix()
	reply.Status = c.servers[args.ServerID].status
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// fmt.Printf("RequestTask from server %d\n", args.ServerID)
	uncompletedTaskID := -1
	c.mu.Lock()
	defer c.mu.Unlock()

	for idx, mapTask := range c.mapTasks {
		if mapTask.status != Idle {
			if mapTask.status == InProcess {
				uncompletedTaskID = idx
			}
			continue
		}
		c.mapTasks[idx].serverID = args.ServerID
		c.mapTasks[idx].status = InProcess
		reply.TaskType = 0
		reply.TaskID = uint(idx)
		reply.NReduce = len(c.reduceTasks)
		reply.Filename = mapTask.filename

		c.servers[args.ServerID].taskType = 0
		c.servers[args.ServerID].taskID = idx
		c.servers[args.ServerID].status = WithTask
		// fmt.Printf("Server %d get task %d of type %d\n", args.ServerID, reply.TaskID, reply.TaskType)
		return nil
	}

	if uncompletedTaskID != -1 {
		reply.TaskType = 2
		// fmt.Printf("Wait for Task %d of type 0 executed by server %d\n", uncompletedTaskID, c.mapTasks[uncompletedTaskID].serverID)
		return nil
	}

	for idx, reduceTask := range c.reduceTasks {
		if reduceTask.status != Idle {
			if reduceTask.status == InProcess {
				uncompletedTaskID = idx
			}
			continue
		}
		c.reduceTasks[idx].serverID = args.ServerID
		c.reduceTasks[idx].status = InProcess

		reply.TaskType = 1
		reply.NMap = len(c.mapTasks)
		reply.TaskID = uint(idx)
		c.servers[args.ServerID].taskType = 1
		c.servers[args.ServerID].taskID = idx
		c.servers[args.ServerID].status = WithTask
		// fmt.Printf("Server %d get task %d of type %d\n", args.ServerID, reply.TaskID, reply.TaskType)
		return nil
	}

	if uncompletedTaskID != -1 {
		reply.TaskType = 2
		// fmt.Printf("Wait for Task %d of type 1 executed by server %d\n", uncompletedTaskID, c.reduceTasks[uncompletedTaskID].serverID)
		return nil
	}

	reply.TaskType = 3
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	// fmt.Printf("ReportTask %d of type %d from server %d\n", args.TaskID, args.TaskType, args.ServerID)
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == 0 {
		c.mapTasks[args.TaskID].status = Completed
	} else {
		c.reduceTasks[args.TaskID].status = Completed
	}
	c.servers[args.ServerID].status = NoTask
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
	// fmt.Printf("Done Check!!!\n")
	now := time.Now().Unix()
	c.mu.Lock()
	defer c.mu.Unlock()
	for idx, server := range c.servers {
		if server.status == Faulty {
			continue
		}
		// fmt.Printf("Server %d lastHeartbeat %d now %d\n", server.serverID, server.lastHeartbeat, now)
		if server.lastHeartbeat < now - 5 {
			// fmt.Printf("Server %d is faulty current status %d \n", server.serverID, server.status)
			if server.status == WithTask {
				// fmt.Printf("Task %d of type %d belongs server %d\n", server.taskID, server.taskType, c.mapTasks[server.taskID].serverID)
				if server.taskType == 0 {
					if c.mapTasks[server.taskID].serverID == server.serverID {
						c.mapTasks[server.taskID].status = Idle
						// fmt.Printf("Task %d of type %d needs redo\n", server.taskID, server.taskType)
					}
				} else {
					if c.reduceTasks[server.taskID].serverID == server.serverID && c.reduceTasks[server.taskID].status == InProcess {
						c.reduceTasks[server.taskID].status = Idle
						// fmt.Printf("Task %d of type %d needs redo\n", server.taskID, server.taskType)
					}
				}
			}
			c.servers[idx].status = Faulty
		}
	}

	ret := true

	// Your code here.
	for _, task := range c.reduceTasks {
		if task.status != Completed {
			return false
		}
	}

	for _, task := range c.mapTasks {
		if task.status != Completed {
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
	c.servers = make([]Server, 0)
	c.mapTasks = make([]MapTask, len(files))
	c.reduceTasks = make([]ReduceTask, nReduce)

	for idx, filename := range files {
		c.mapTasks[idx].status = Idle
		c.mapTasks[idx].filename = filename
	}

	for idx := 0; idx < nReduce; idx++ {
		c.reduceTasks[idx].status = Idle
	}
	// print("nMap: ", len(files), "\n")
	// print("nReduce: ", nReduce, "\n")
	c.server()
	return &c
}

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RequestTaskArgs struct {

}

type RequestTaskReply struct {
	TaskType uint8
	TaskID uint
	NMap int
	NReduce int
	Filename string
}

type ReportTaskArgs struct {
	TaskType uint8
	TaskID uint
	Output string
}

type ReportTaskReply struct {
	TaskType uint8
	TaskID uint
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

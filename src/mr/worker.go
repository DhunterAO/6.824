package mr

import "fmt"
import "log"
import "os"
import "io/ioutil"
import "net/rpc"
import "hash/fnv"
import "time"
import "sort"
import "encoding/json"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reply := RegisterServerReply{}
	call("Coordinator.RegisterServer", &RegisterServerArgs{}, &reply)
	serverID := reply.ServerID
	// Your worker implementation here.
	go func() {
		for {
			time.Sleep(time.Second)
			heartbeatReply := HeartbeatReply{}
			call("Coordinator.Heartbeat", &HeartbeatArgs{ServerID: serverID}, &heartbeatReply)
		}
	}()

	for {
		request := RequestTaskArgs{
			ServerID: serverID,
		}
		reply := RequestTaskReply{}
		call("Coordinator.RequestTask", &request, &reply)
		if reply.TaskType == 2 {
			continue
		} else if reply.TaskType == 3 { // no more work
			return
		} else if reply.TaskType == 0 {
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))
			for _, kv := range kva {
				keyID := ihash(kv.Key) % reply.NReduce
				tempFileName := fmt.Sprintf("mr-mid-%d-%v-%v", serverID, reply.TaskID, keyID)
				tempFile, err := os.OpenFile(tempFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("cannot open %v", tempFileName)
				}
				enc := json.NewEncoder(tempFile)
				enc.Encode(&kv)
				tempFile.Close()
			}

			for keyID := 0; keyID < reply.NReduce; keyID++ {
				tempFileName := fmt.Sprintf("mr-mid-%d-%v-%v", serverID, reply.TaskID, keyID)
				midFileName := fmt.Sprintf("mr-mid-%v-%v", reply.TaskID, keyID)
				os.Rename(tempFileName, midFileName)
			}

			report := ReportTaskArgs{
				ServerID: serverID,
				TaskType: reply.TaskType,
				TaskID:   reply.TaskID,
			}
			call("Coordinator.ReportTask", &report, &ReportTaskReply{})
		} else if reply.TaskType == 1 {
			kva := []KeyValue{}
			for mapID := 0; mapID < reply.NMap; mapID++ {
				midFileName := fmt.Sprintf("mr-mid-%v-%v", mapID, reply.TaskID)
				midFile, err := os.OpenFile(midFileName, os.O_RDONLY, 0644)
				defer midFile.Close()
				if err != nil {
					continue
				}

				dec := json.NewDecoder(midFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%v", reply.TaskID)
			tempFile, err := ioutil.TempFile(".", oname)
			if err != nil {
				log.Fatalf("cannot open %v", oname)
			}

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			tempname := tempFile.Name()
			tempFile.Close()
			os.Rename(tempname, oname)

			report := ReportTaskArgs{
				ServerID: serverID,
				TaskType: reply.TaskType,
				TaskID:   reply.TaskID,
			}
			call("Coordinator.ReportTask", &report, &ReportTaskReply{})
		} else {
			log.Fatalf("unknown task type %v", reply.TaskType)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 1

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

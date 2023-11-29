package mr

import "fmt"
import "log"
import "os"
import "io/ioutil"
import "net/rpc"
import "hash/fnv"
import "time"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
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
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// fmt.Printf("worker start!\n")

	// Your worker implementation here.
	for {
		request := RequestTaskArgs{}
		reply := RequestTaskReply{}
		call("Coordinator.RequestTask", &request, &reply)
		// fmt.Printf("worker get task %v of type %v\n", reply.TaskID, reply.TaskType)
		if reply.TaskType == 2 {
			time.Sleep(time.Second)
		} else if reply.TaskType == 3 { // no more work
			return
		} else if reply.TaskType == 0 {
			filename := reply.Filename
			// fmt.Printf("filename %v\n", filename)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			// fmt.Println(string(content))
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			for _, kv := range kva {
				keyID := ihash(kv.Key) % reply.NReduce
				midFileName := fmt.Sprintf("mid-%v-%v", reply.TaskID, keyID)
				midFile, err := os.OpenFile(midFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("cannot open %v", midFileName)
				}
				// fmt.Println(kv.Key, kv.Value)
				fmt.Fprintf(midFile, "%v %v\n", kv.Key, kv.Value)
				midFile.Close()
			}

			report := ReportTaskArgs{
				TaskType: reply.TaskType,
				TaskID: reply.TaskID,
			}
			call("Coordinator.ReportTask", &report, &ReportTaskReply{})
		} else if reply.TaskType == 1 {
			intermediate := []KeyValue{}
			for mapID := 0; mapID < reply.NMap; mapID++ {
				midFileName := fmt.Sprintf("mid-%v-%v", mapID, reply.TaskID)
				midFile, err := os.OpenFile(midFileName, os.O_RDONLY, 0644)
				if err != nil {
					log.Fatalf("cannot read %v", midFileName)
				}
				var key string
				var value string

				for ;; {
					_, err := fmt.Fscanf(midFile, "%s %s\n", &key, &value)
					if err != nil {
						break
					}
					intermediate = append(intermediate, KeyValue{key, value})
				}
				
				// fmt.Println(key, value)
				intermediate = append(intermediate, KeyValue{key, value})
			}
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", reply.TaskID)
			// fmt.Println(oname)
			// fmt.Println(intermediate)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}

			ofile.Close()

			report := ReportTaskArgs{
				TaskType: reply.TaskType,
				TaskID: reply.TaskID,
			}
			call("Coordinator.ReportTask", &report, &ReportTaskReply{})
		} else {
			log.Fatalf("unknown task type %v", reply.TaskType)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

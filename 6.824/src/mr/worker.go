package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "encoding/json"
import "sort"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

func interFilename(id, r int) string {
	return fmt.Sprintf("mr-%d-%d", id, r)
}

func outFilename(id int) string {
	return fmt.Sprintf("mr-out-%d", id)
}

func runMap(t Task, mapf func(string, string) []KeyValue) {
	intermediate := make([][]KeyValue, t.R)
	for _, filename := range t.Files {
		content, err := ioutil.ReadFile(filename) // TODO GFS
		if err != nil {
			// TODO report fail
		}
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			r := ihash(kv.Key) % t.R
			intermediate[r] = append(intermediate[r], kv)
		}
	}
	
	// TODO combine same key
	
	interFiles := make([]string, t.R)
	for r := 0; r < t.R; r++ {
		if len(intermediate[r]) > 0 {
			filename := interFilename(t.Id, r)
			interFiles[r] = filename
			f, err := os.Create(filename)
			if err != nil {
				// TODO report fail
			}
			enc := json.NewEncoder(f)
			enc.Encode(intermediate[r])
			f.Close()
		}
	}
	
	t.Files = interFiles
	call("Master.MapFeedback", &t, &t)
}

func runReduce(t Task, reducef func(string, []string) string) {
	if len(t.Files) == 0 {
		call("Master.ReduceFeedback", &t, &t)
		return
	}
	
	ofile, err := ioutil.TempFile(".", "mr-tmp-") // TODO GFS
	if err != nil {
		// TODO report fail
	}
	
	var kva []KeyValue
	for _, filename := range t.Files {
		f, err := os.Open(filename) // TODO remote read
		if err != nil {
			// TODO report fail
		}
		dec := json.NewDecoder(f)
		var tmp []KeyValue
		dec.Decode(&tmp)
		kva = append(kva, tmp...)
	}
	
	// TODO may external sort
	sort.Sort(ByKey(kva))
	
	key := kva[0].Key
	values := []string{kva[0].Value}
	for _, kv := range kva[1:] {
		if kv.Key == key {
			values = append(values, kv.Value)
		} else {
			output := reducef(key, values)
			fmt.Fprintf(ofile, "%v %v\n", key, output)
			
			key = kv.Key
			values = []string{kv.Value}
		}
	}
	
	if len(values) > 0 {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	
	os.Rename(ofile.Name(), outFilename(t.Id))

	call("Master.ReduceFeedback", &t, &t)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		t := Task{}
		call("Master.Assign", &t, &t)

		// TODO heart beat
		
		switch t.Phase {
		case MapPhase:
			runMap(t, mapf)
		case ReducePhase:
			runReduce(t, reducef)
		default:
			time.Sleep(1)
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Fatal("rpc.Client.Call:", err)
	}
}

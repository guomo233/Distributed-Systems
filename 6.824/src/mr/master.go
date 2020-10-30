package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
//import "fmt"

type taskState struct {
	completed bool
	timer *time.Timer
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	Idle
)

type Misson struct {
	Id int
	Files []string
}

type Task struct {
	Phase
	Misson
	R int
}

type Master struct {
	// Your definitions here.
	mMisson []Misson
	rMisson []Misson
	mTasks []taskState
	rTasks []taskState
	mCompleted int
	rCompleted int
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

func (m *Master) mapDone() bool {
	return m.mCompleted == len(m.mTasks)
}

func (m *Master) reduceDone() bool {
	return m.rCompleted == len(m.rTasks)
}

func (m *Master) ReduceFeedback(t *Task, _ *Task) error {
	m.rTasks[t.Id].timer.Stop()
	
	mu.Lock()
	defer mu.Unlock()
	
	if m.rTasks[t.Id].completed {
		return nil
	}
	
	m.rTasks[t.Id].completed = true
	m.rCompleted++
	return nil
}

func (m *Master) MapFeedback(t *Task, _ *Task) error {
	m.mTasks[t.Id].timer.Stop()
	
	mu.Lock()
	defer mu.Unlock()
	
	if m.mTasks[t.Id].completed {
		return nil
	}
	
	for r, f := range t.Files {
		if f != "" {
			m.rMisson[r].Files = append(m.rMisson[r].Files, f)
		}
	}
	
	m.mTasks[t.Id].completed = true
	m.mCompleted++
	return nil
}

func (m *Master) mapCrush (t *Task) {
	<- m.mTasks[t.Id].timer.C
	
	mu.Lock()
	defer mu.Unlock()
	
	m.mMisson = append(m.mMisson, t.Misson)
}

func (m *Master) reduceCrush (t *Task) {
	<- m.rTasks[t.Id].timer.C
	
	mu.Lock()
	defer mu.Unlock()
	
	m.rMisson = append(m.rMisson, t.Misson)
}

func (m *Master) Assign(_ *Task, t *Task) error {
	mu.Lock()
	defer mu.Unlock()
	
	if len(m.mMisson) > 0 {
		t.Phase = MapPhase
		t.R = len(m.rTasks)
		t.Misson = m.mMisson[0]
		m.mMisson = m.mMisson[1:]
		timer := time.NewTimer(10 * time.Second)
		m.mTasks[t.Id] = taskState{false, timer}
		go m.mapCrush(t)
	} else if m.mapDone() && len(m.rMisson) > 0 {
		t.Phase = ReducePhase
		t.Misson = m.rMisson[0]
		m.rMisson = m.rMisson[1:]
		timer := time.NewTimer(10 * time.Second)
		m.rTasks[t.Id] = taskState{false, timer}
		go m.reduceCrush(t)
	} else {
		t.Phase = Idle
	}
	
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
//	fmt.Printf("mMission: %d mTasks: %d mCompleted: %d\nrMisson: %d rTasks: %d rCompleted: %d\n\n", len(m.mMisson), len(m.mTasks), m.mCompleted, len(m.rMisson), len(m.rTasks), m.rCompleted)
	return len(m.mMisson) == 0 && len(m.rMisson) == 0 &&  m.mapDone() && m.reduceDone()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// TODO split files by size
	m.mTasks = make([]taskState, len(files))
	for i := range files {
		m.mMisson = append(m.mMisson, Misson{i, files[i:i+1]})
	}
	m.rTasks = make([]taskState, nReduce)
	for i := 0; i < nReduce; i++ {
		m.rMisson = append(m.rMisson, Misson{i, nil})
	}
	
	m.server()
	return &m
}
package mapreduce

import (
	"hash/fnv"
	"strconv"

	"github.com/golang/glog"
)

// Debugging enabled?
const debugEnabled = false

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) {
	if debugEnabled {
		glog.Infof(format, a...)
	}
}

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

type pairs []KeyValue

func (s pairs) Less(i, j int) bool {
	return s[i].Key < s[j].Key
}

func (s pairs) Len() int {
	return len(s)
}

func (s pairs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase jobPhase = "reducePhase"
)

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

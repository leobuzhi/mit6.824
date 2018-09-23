package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/golang/glog"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	data, err := ioutil.ReadFile(inFile)
	if err != nil {
		glog.Errorf("os.Open %s failed,err: %v", inFile, err)
	}

	medFiles := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	for r := 0; r != nReduce; r++ {
		rName := reduceName(jobName, mapTask, r)
		medFiles[r], err = os.Create(rName)
		if err != nil {
			glog.Errorf("os.Create %v faild,err: %v", rName, err)
		}
		defer medFiles[r].Close()
		encs[r] = json.NewEncoder(medFiles[r])
	}

	kvs := mapF(inFile, string(data))
	for _, kv := range kvs {
		i := ihash(kv.Key) % nReduce
		if err = encs[i].Encode(kv); err != nil {
			glog.Errorf("enc.Encode err: %v", err)
		}
	}
}

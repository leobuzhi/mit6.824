package mapreduce

import (
	"bufio"
	"encoding/json"
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
	for r := 0; r != nReduce; r++ {
		iFile, err := os.Open(inFile)
		if err != nil {
			glog.Errorf("os.Open %s failed,err: %v", inFile, err)
		}

		rName := reduceName(jobName, mapTask, r)
		oFile, err := os.Create(rName)
		if err != nil {
			glog.Errorf("os.Create %v faild,err: %v", rName, err)
		}
		iFileScanner := bufio.NewScanner(iFile)
		enc := json.NewEncoder(oFile)
		for iFileScanner.Scan() {
			text := iFileScanner.Text()
			if ihash(text)%nReduce == r {
				err := enc.Encode(&KeyValue{Key: text, Value: ""})
				if err != nil {
					glog.Errorf("enc.Encode err: %v", err)
				}
			}
		}
	}
}

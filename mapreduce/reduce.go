package mapreduce

import (
	"encoding/json"
	"io"
	"os"
	"sort"

	"github.com/golang/glog"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	var kvs []KeyValue
	for m := 0; m < nMap; m++ {
		rName := reduceName(jobName, m, reduceTask)
		iFile, err := os.Open(rName)
		if err != nil {
			glog.Errorf("os.Open %s failed,err: %v", rName, err)
		}
		defer iFile.Close()

		var kv KeyValue
		dec := json.NewDecoder(iFile)
		for {
			if err = dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				glog.Fatalf("json.Decode %s failed,err: %v", rName, err)
			}
			kvs = append(kvs, kv)
		}
	}

	sort.Sort(pairs(kvs))
	for _, v := range kvs {
		reduceF(v.Key, []string{v.Value})
	}

	oFile, err := os.Create(mergeName(jobName, reduceTask))
	if err != nil {
		glog.Errorf("os.Create %v faild,err: %v", mergeName(jobName, reduceTask), err)
	}
	defer oFile.Close()

	enc := json.NewEncoder(oFile)
	for _, kv := range kvs {
		err = enc.Encode(&KeyValue{Key: kv.Key, Value: reduceF(kv.Key, []string{kv.Value})})
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.Fatalf("enc.Encode err: %v", err)
		}
	}
}

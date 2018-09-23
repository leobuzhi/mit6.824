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
	var err error

	medFiles := make([]*os.File, nMap)
	decs := make([]*json.Decoder, nMap)
	for n := 0; n < nMap; n++ {
		rName := reduceName(jobName, n, reduceTask)
		medFiles[n], err = os.Open(rName)
		if err != nil {
			glog.Errorf("os.Open %s failed,err: %v", rName, err)
		}
		defer medFiles[n].Close()
		decs[n] = json.NewDecoder(medFiles[n])
	}

	medKVs := make(map[string][]string)
	var kv KeyValue
	for i := range medFiles {
		for {
			if err = decs[i].Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				glog.Fatalf("json.Decode %s failed,err: %v", medFiles[i].Name(), err)
			}
			medKVs[kv.Key] = append(medKVs[kv.Key], kv.Value)
		}
	}

	keys := make([]string, 0, len(medKVs))
	for k := range medKVs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	oFile, err := os.Create(outFile)
	if err != nil {
		glog.Errorf("os.Create %v faild,err: %v", outFile, err)
	}
	defer oFile.Close()

	enc := json.NewEncoder(oFile)
	for _, key := range keys {
		if err = enc.Encode(KeyValue{Key: key, Value: reduceF(key, medKVs[key])}); err != nil {
			if err == io.EOF {
				break
			}
			glog.Fatalf("enc.Encode err: %v", err)
		}
	}
}

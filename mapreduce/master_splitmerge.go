package mapreduce

import (
	"os"

	"github.com/golang/glog"
)

// removeFile is a simple wrapper around os.Remove that logs errors.
func removeFile(n string) {
	err := os.Remove(n)
	if err != nil {
		glog.Fatal("CleanupFiles ", err)
	}
}

// CleanupFiles removes all intermediate files produced by running mapreduce.
func (mr *Master) CleanupFiles() {
	for i := range mr.files {
		for j := 0; j < mr.nReduce; j++ {
			removeFile(reduceName(mr.jobName, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		removeFile(mergeName(mr.jobName, i))
	}
	removeFile("mrtmp." + mr.jobName)
}

func cleanup(mr *Master) {
	mr.CleanupFiles()
	for _, f := range mr.files {
		removeFile(f)
	}
}

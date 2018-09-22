package mapreduce

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
)

const (
	nNumber = 100000
	nMap    = 20
	nReduce = 10
)

// Split in words
func MapFunc(file string, value string) (res []KeyValue) {
	debug("Map %v\n", value)
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}

// Just return key
func ReduceFunc(key string, values []string) string {
	for _, e := range values {
		debug("Reduce %s %v\n", key, e)
	}
	return ""
}

// Make input file
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			glog.Fatal("mkInput: ", err)
		}
		w := bufio.NewWriter(file)
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}

// Checks input file agaist output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, files []string) {
	output, err := os.Open("mrtmp.test")
	assert.Equal(t, nil, err)
	defer output.Close()

	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		assert.Equal(t, nil, err)
		defer input.Close()

		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}

		assert.Equal(t, nil, err)
		assert.Equal(t, v1, v2)
		i++
	}
	assert.Equal(t, i, nNumber)
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 DoTask RPC.
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		assert.NotEqual(t, 0, tasks)
	}
}

func TestSequentialSingle(t *testing.T) {
	mr := Sequential("test", makeInputs(1), 1, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestSequentialMany(t *testing.T) {
	mr := Sequential("test", makeInputs(5), 3, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

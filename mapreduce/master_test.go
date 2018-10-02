package mapreduce

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
)

const (
	nNumber = 100000
	nMap    = 20
	nReduce = 10
)

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *Master {
	files := makeInputs(nMap)
	master := port("master")
	mr := Distributed("test", files, nReduce, master)
	return mr
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

func TestParallelBasic(t *testing.T) {
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1, nil)
	}
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestParallelCheck(t *testing.T) {
	mr := setup()
	parallelism := &Parallelism{}
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1, parallelism)
	}
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)

	parallelism.mu.Lock()
	if parallelism.max < 2 {
		t.Fatalf("workers did not execute in parallel")
	}
	parallelism.mu.Unlock()

	cleanup(mr)
}

func TestOneFailure(t *testing.T) {
	mr := setup()
	// Start 2 workers that fail after 10 tasks
	go RunWorker(mr.address, port("worker"+strconv.Itoa(0)),
		MapFunc, ReduceFunc, 10, nil)
	go RunWorker(mr.address, port("worker"+strconv.Itoa(1)),
		MapFunc, ReduceFunc, -1, nil)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.doneChannel:
			check(t, mr.files)
			cleanup(mr)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 tasks
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10, nil)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10, nil)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}

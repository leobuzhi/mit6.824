package main

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

var tm map[string]int

func TestMainFunc(t *testing.T) {
	tcs := []struct {
		args []string
		err  error
	}{
		{
			[]string{"main.go", "master", "sequential", "pg-being_ernest.txt", "pg-dorian_gray.txt", "pg-frankenstein.txt", "pg-grimm.txt",
				"pg-huckleberry_finn.txt", "pg-metamorphosis.txt", "pg-sherlock_holmes.txt", "pg-tom_sawyer.txt"},
			nil,
		},
	}

	for _, tc := range tcs {
		os.Args = tc.args
		main()
		cmd := "sort -n -k2 mrtmp.wcseq | tail -10 | diff - mr-testout.txt"
		out, err := exec.Command("bash", "-c", cmd).Output()
		assert.Equal(t, nil, err)
		assert.Equal(t, "", string(out))
	}
	mr.CleanupFiles()
}

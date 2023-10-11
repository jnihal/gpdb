package testutils

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func Equal(t *testing.T, expected, actual interface{}) bool {
	if expected != actual {
		t.Errorf("\nExpected: %#v \nGot: %v", expected, actual)
	}
	return true
}

func NotNil(t *testing.T, actual interface{}) bool {
	if actual == nil {
		t.Errorf("\nExpected error Got: %#v", actual)
	}
	return true
}

func EqualValues(t *testing.T, expected, actual interface{}) bool {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("\nExpected: %v \nGot: %v", StructToString(expected), StructToString(actual))
	}
	return true
}

func Contains(t *testing.T, expected []string, actual string) bool {
	for _, item := range expected {
		if !strings.Contains(actual, item) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", item, actual)
		}
	}
	return true
}

func NotContains(t *testing.T, expected string, actual string) bool {
	if strings.Contains(actual, expected) {
		t.Errorf("\nUnexpected string: %#v \nFound in: %#v", expected, actual)
	}
	return true
}

func FileExists(t *testing.T, file string) bool {
	if _, err := os.Stat(file); err != nil {
		t.Errorf("File %s not found", file)
	}
	return true
}

func SvcFilesExistsOnRemoteHosts(t *testing.T, file string, hosts []string) bool {
	genCmd := Command{
		cmdStr: fmt.Sprintf("test -e %s && echo $?", file),
	}
	for _, host := range hosts {
		genCmd.host = host
		out, _, _ := runCmd(genCmd)
		if strings.TrimSpace(out) != "0" {
			t.Errorf("File %s not found on %s", file, host)
		}
	}
	return true
}

func ServiceFilesExist(t *testing.T, files ...string) bool {
	for _, file := range files {
		return FileExists(t, file)
	}
	return false
}

func VerifyServicePIDOnPort(t *testing.T, PidStatus string, port int, host string) bool {
	var pid string
	if _, err := strconv.Atoi(PidStatus); err != nil {
		pid = extractPID(PidStatus)
	} else {
		pid = PidStatus
	}

	listeningPid := GetListeningProcess(port, host)
	if pid != listeningPid {
		t.Errorf("pid %s in service status not matching with pid(%s) listening on port %d ", pid, listeningPid, port)
	}

	return true
}

func VerifySvcNotRunning(t *testing.T, svcStatus string) bool {
	pid := extractPID(svcStatus)
	if pid != "0" {
		t.Errorf("service is still running with pid %s", pid)
	}
	return true
}

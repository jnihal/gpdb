package utils

import (
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
)

var System = InitializeSystemFunctions()
var ExecuteAndGetUlimit = ExecuteAndGetUlimitFn

type SystemFunctions struct {
	CurrentUser    func() (*user.User, error)
	InterfaceAddrs func() ([]net.Addr, error)
	Open           func(name string) (*os.File, error)
	Create         func(name string) (*os.File, error)
	WriteFile      func(name string, data []byte, perm fs.FileMode) error
	ExecCommand    func(name string, arg ...string) *exec.Cmd
	Getuid         func() int
	Stat           func(name string) (os.FileInfo, error)
	Getgid         func() int
	RemoveAll      func(path string) error
	ReadFile       func(name string) ([]byte, error)
}

func InitializeSystemFunctions() *SystemFunctions {
	return &SystemFunctions{
		CurrentUser:    user.Current,
		InterfaceAddrs: net.InterfaceAddrs,
		Open:           os.Open,
		Create:         os.Create,
		WriteFile:      os.WriteFile,
		ExecCommand:    exec.Command,
		Getuid:         os.Geteuid,
		Stat:           os.Stat,
		Getgid:         os.Getgid,
		RemoveAll:      os.RemoveAll,
		ReadFile:       os.ReadFile,
	}
}

func ResetSystemFunctions() {
	System = InitializeSystemFunctions()
}

/*
		WriteLinesToFile creates a new file with the given contents.
		If a file with the name already exists, overwrites the file with new contents.
	    Takes lines to be written as input and updates to the file by adding \n's.
*/
func WriteLinesToFile(filename string, lines []string) error {
	file, err := System.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(strings.Join(lines, "\n"))
	if err != nil {
		return err
	}

	return nil
}

func GetHostAddrsNoLoopback() ([]string, error) {
	var addrs []string
	ipAddresses, err := System.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, ip := range ipAddresses {
		if ipnet, ok := ip.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			addrs = append(addrs, ip.String())
		}
	}

	return addrs, nil
}

/*
GetListDifference returns all the elements present in listA but not listB
*/
func GetListDifference(listA, listB []string) []string {
	var result []string
	m := make(map[string]bool)

	for _, item := range listB {
		m[item] = true
	}

	for _, item := range listA {
		if _, ok := m[item]; !ok {
			result = append(result, item)
		}
	}

	return result
}

func ExecuteAndGetUlimitFn() (int, error) {
	out, err := System.ExecCommand("ulimit", "-n").CombinedOutput()
	if err != nil {
		return -1, fmt.Errorf("error fetching open file limit values:%v", err)
	}

	ulimitVal, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return -1, fmt.Errorf("could not convert the ulimit value: %v", err)
	}
	return ulimitVal, nil
}
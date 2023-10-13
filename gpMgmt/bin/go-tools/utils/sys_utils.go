package utils

import (
	"io/fs"
	"net"
	"os"
	"os/exec"
	"os/user"
	"strings"
)

var System = InitializeSystemFunctions()

type SystemFunctions struct {
	CurrentUser    func() (*user.User, error)
	InterfaceAddrs func() ([]net.Addr, error)
	Open           func(name string) (*os.File, error)
	Create         func(name string) (*os.File, error)
	WriteFile      func(name string, data []byte, perm fs.FileMode) error
	ExecCommand    func(name string, arg ...string) *exec.Cmd
}

func InitializeSystemFunctions() *SystemFunctions {
	return &SystemFunctions{
		CurrentUser:    user.Current,
		InterfaceAddrs: net.InterfaceAddrs,
		Open:           os.Open,
		Create:         os.Create,
		WriteFile:      os.WriteFile,
		ExecCommand:    exec.Command,
	}
}

func ResetSystemFunctions() {
	System = InitializeSystemFunctions()
}

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

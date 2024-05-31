package greenplum

import (
	"os/exec"

	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
)

const (
	gpstart = "gpstart"
	gpsync  = "gpsync"
	gpssh   = "gpssh"
)

type GpStart struct {
	DataDirectory string `flag:"-d"`
	Verbose       bool   `flag:"-v"`
}

func (cmd *GpStart) BuildExecCommand(gpHome string) *exec.Cmd {
	utility := utils.GetGpUtilityPath(gpHome, gpstart)
	args := append([]string{"-a"}, utils.GenerateArgs(cmd)...)

	return utils.System.ExecCommand(utility, args...)
}

type GpSync struct {
	Hostnames   []string
	Source      string
	Destination string
}

func (cmd *GpSync) BuildExecCommand(gpHome string) *exec.Cmd {
	utility := utils.GetGpUtilityPath(gpHome, gpsync)
	args := []string{}

	for _, host := range cmd.Hostnames {
		args = append(args, "-h", host)
	}

	args = append(args, cmd.Source, cmd.Destination)

	return exec.Command(utility, args...)
}

type GpSSH struct {
	Hostnames []string
	Command   string
}

func (cmd *GpSSH) BuildExecCommand(gpHome string) *exec.Cmd {
	utility := utils.GetGpUtilityPath(gpHome, gpssh)
	args := []string{}

	for _, host := range cmd.Hostnames {
		args = append(args, "-h", host)
	}

	args = append(args, cmd.Command)

	return exec.Command(utility, args...)
}

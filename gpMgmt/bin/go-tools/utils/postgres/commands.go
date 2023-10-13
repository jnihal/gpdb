package postgres

import (
	"os/exec"
	"path"
	"strconv"

	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/hub"
)

const (
	initdbUtility = "initdb"
	pgCtlUtility  = "pg_ctl"
)

func GetGphomePath() (string, error) {
	gpConf := &hub.Config{}
	err := gpConf.Load(cli.ConfigFilePath)
	if err != nil {
		return "", err
	}
	
	return gpConf.GpHome, nil
}

func GetGphomeUtilityPath(utility string) (string, error) {
	gphome, err := GetGphomePath()
	if err != nil {
		return "", err
	}

	return path.Join(gphome, "bin", utility), nil
}

// Initdb represents the initdb command configuration.
type Initdb struct {
	PgData         string
	Encoding       string
	SharedBuffers  string
	MaxConnections int
	locale         map[string]string
}

// GetCmd returns an exec.Cmd for the initdb command.
func (cmd *Initdb) GetCmd() *exec.Cmd {
	utility, _ := GetGphomeUtilityPath(initdbUtility)
	args := []string{"-D", cmd.PgData}

	if cmd.Encoding != "" {
		args = append(args, "-E", cmd.Encoding)
	}
	if cmd.SharedBuffers != "" {
		args = append(args, "--shared_buffers", cmd.SharedBuffers)
	}
	if cmd.MaxConnections != 0 {
		args = append(args, "--max_connections", strconv.Itoa(cmd.MaxConnections))
	}

	return exec.Command(utility, args...)
}

// PgCtlStart represents the pg_ctl start command configuration.
type PgCtlStart struct {
	PgData  string
	Timeout int
	Wait    bool
	Logfile string
	Options string
}

// GetCmd returns an exec.Cmd for the pg_ctl start command.
func (cmd *PgCtlStart) GetCmd() *exec.Cmd {
	utility := pgCtlUtility
	args := []string{"start", "-D", cmd.PgData}

	if cmd.Timeout != 0 {
		args = append(args, "-t", strconv.Itoa(cmd.Timeout))
	}
	if cmd.Wait {
		args = append(args, "-w")
	} else {
		args = append(args, "-W")
	}
	if cmd.Logfile != "" {
		args = append(args, "-l", cmd.Logfile)
	}
	if len(cmd.Options) > 0 {
		args = append(args, "-o", cmd.Options)
	}

	return exec.Command(utility, args...)
}

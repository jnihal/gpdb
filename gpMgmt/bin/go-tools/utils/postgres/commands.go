package postgres

import (
	"bytes"
	"os/exec"
	"path"
	"strconv"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/utils"
)

const (
	initdbUtility = "initdb"
	pgCtlUtility  = "pg_ctl"
)

type PgCommand interface {
	buildPgCommand(gphome string) *exec.Cmd
}

func NewPgCommand(pgCmd PgCommand, gphome string) *exec.Cmd {
	return pgCmd.buildPgCommand(gphome)
}

func RunPgCommand(pgCmd PgCommand, gphome string) (*bytes.Buffer, error) {
	stdout := new(bytes.Buffer)
    stderr := new(bytes.Buffer)
	
	cmd := NewPgCommand(pgCmd, gphome)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	gplog.Debug("Executing command: %s", cmd.String())	
	err := cmd.Run()
	
	if err != nil {
		return stderr, err
	} else {
		return stdout, err
	}
}

// Initdb represents the initdb command configuration.
type Initdb struct {
	PgData         string
	Encoding       string
	Locale         string
	LcCollate      string
	LcCtype        string
	LcMessages     string
	LcMonetory     string
	LcNumeric      string
	LcTime         string
	SharedBuffers  string
	MaxConnections int
}

// GetCmd returns an exec.Cmd for the initdb command.
func (cmd *Initdb) buildPgCommand(gphome string) *exec.Cmd {
	utility := getGphomeUtilityPath(gphome, initdbUtility)
	args := []string{}

	args = appendIfNotEmpty(args, "--pgdata", cmd.PgData)
	args = appendIfNotEmpty(args, "--encoding", cmd.Encoding)
	args = appendIfNotEmpty(args, "--lc-collate", cmd.LcCollate)
	args = appendIfNotEmpty(args, "--lc-ctype", cmd.LcCtype)
	args = appendIfNotEmpty(args, "--lc-messages", cmd.LcMessages)
	args = appendIfNotEmpty(args, "--lc-monetary", cmd.LcMonetory)
	args = appendIfNotEmpty(args, "--lc-numeric", cmd.LcNumeric)
	args = appendIfNotEmpty(args, "--lc-time", cmd.LcTime)
	args = appendIfNotEmpty(args, "--max_connections", cmd.MaxConnections)
	args = appendIfNotEmpty(args, "--shared_buffers", cmd.SharedBuffers)

	return utils.System.ExecCommand(utility, args...)
}

// PgCtlStart represents the pg_ctl start command configuration.
type PgCtlStart struct {
	PgData  string
	Timeout int
	Wait    bool
	NoWait  bool
	Logfile string
	Options string
	Mode    string
}

// GetCmd returns an exec.Cmd for the pg_ctl start command.
func (cmd *PgCtlStart) buildPgCommand(gphome string) *exec.Cmd {
	utility := getGphomeUtilityPath(gphome, pgCtlUtility)
	args := []string{"start"}

	args = appendIfNotEmpty(args, "--pgdata", cmd.PgData)
	args = appendIfNotEmpty(args, "--timeout", cmd.Timeout)
	args = appendIfNotEmpty(args, "--wait", cmd.Wait)
	args = appendIfNotEmpty(args, "--no-wait", cmd.NoWait)
	args = appendIfNotEmpty(args, "--log", cmd.Logfile)
	args = appendIfNotEmpty(args, "--options", cmd.Options)
	args = appendIfNotEmpty(args, "--mode", cmd.Mode)

	return utils.System.ExecCommand(utility, args...)
}

func getGphomeUtilityPath(gphome, utility string) string {
	return path.Join(gphome, "bin", utility)
}

func appendIfNotEmpty(args []string, flag string, value interface{}) []string {
	switch value := value.(type) {
	case int:
		if value != 0 {
			args = append(args, flag, strconv.Itoa(value))
		}
	case float64:
		if value != 0 {
			args = append(args, flag, strconv.FormatFloat(value, 'f', -1, 64))
		}
	case string:
		if value != "" {
			args = append(args, flag, value)
		}
	case bool:
		if value {
			args = append(args, flag)
		}
	}

	return args
}

package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

func (s *Server) PgBasebackup(ctx context.Context, req *idl.PgBasebackupRequest) (*idl.PgBasebackupResponse, error) {
	pgBasebackupCmd := &postgres.PgBasebackup{
		TargetDir:           req.TargetDir,
		SourceHost:          req.SourceHost,
		SourcePort:          int(req.SourcePort),
		CreateSlot:          req.CreateSlot,
		ForceOverwrite:      req.ForceOverwrite,
		TargetDbid:          int(req.TargetDbid),
		WriteRecoveryConf:   req.WriteRecoveryConf,
		ReplicationSlotName: req.ReplicationSlotName,
		ExcludePaths:        req.ExcludePaths,
	}

	if pgBasebackupCmd.CreateSlot {
		// Drop any previously created slot to avoid error when creating a new slot with the same name.
		err := postgres.DropSlotIfExists(pgBasebackupCmd.SourceHost, pgBasebackupCmd.SourcePort, pgBasebackupCmd.ReplicationSlotName)
		if err != nil {
			return &idl.PgBasebackupResponse{}, fmt.Errorf("failed to drop replication slot %s: %w", pgBasebackupCmd.ReplicationSlotName, err)
		}
	}

	// TODO Check if the directory is empty if ForceOverwrite is false

	filename := filepath.Join(s.LogDir, fmt.Sprintf("pg_basebackup.%s.dbid%d.out", time.Now().Format("20060102_150405"), req.TargetDbid))
	out, err := utils.RunGpCommandAndRedirectOutput(pgBasebackupCmd, s.GpHome, filename)
	if err != nil {
		return &idl.PgBasebackupResponse{}, fmt.Errorf("executing pg_basebackup: %s, logfile: %s, %w", out, filename, err)
	}
	os.Remove(filename)

	return &idl.PgBasebackupResponse{}, nil
}

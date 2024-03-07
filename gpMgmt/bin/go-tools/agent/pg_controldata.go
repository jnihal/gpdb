package agent

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

func (s *Server) PgControlData(ctx context.Context, req *idl.PgControlDataRequest) (*idl.PgControlDataResponse, error) {
	pgControlDataCmd := &postgres.PgControlData{
		PgData: req.Pgdata,
	}
	out, err := utils.RunGpCommand(pgControlDataCmd, s.GpHome)
	if err != nil {
		return &idl.PgControlDataResponse{}, fmt.Errorf("executing pg_controldata: %s, %w", out, err)
	}

	result := make(map[string]string)
	scanner := bufio.NewScanner(out)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}

	return &idl.PgControlDataResponse{Result: result}, nil
}

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

// PgControlData is an agent RPC implementation that executes the pg_controldata command
// and returns the result in form of a map of key-value pairs.
func (s *Server) PgControlData(ctx context.Context, req *idl.PgControlDataRequest) (*idl.PgControlDataResponse, error) {
	pgControlDataCmd := &postgres.PgControlData{
		PgData: req.Pgdata,
	}
	out, err := utils.RunGpCommand(pgControlDataCmd, s.GpHome)
	if err != nil {
		return &idl.PgControlDataResponse{}, fmt.Errorf("executing pg_controldata: %s, %w", out, err)
	}

	pgControlDataOutput := make(map[string]string)
	scanner := bufio.NewScanner(out)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		pgControlDataOutput[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}

	// return only the requested information
	result := make(map[string]string)
	for _, param := range req.Params {
		if _, ok := pgControlDataOutput[param]; !ok {
			return &idl.PgControlDataResponse{}, fmt.Errorf("did not find %q in pg_controldata output", param)
		}

		result[param] = pgControlDataOutput[param]
	}

	return &idl.PgControlDataResponse{Result: result}, nil
}

package agent

import (
	"context"
	"fmt"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

func (s *Server) UpdatePgConf(ctx context.Context, req *idl.UpdatePgConfRequest) (*idl.UpdatePgConfRespoonse, error) {
	err := postgres.UpdatePostgresqlConf(req.Pgdata, req.Params, req.Overwrite)
	if err != nil {
		return &idl.UpdatePgConfRespoonse{}, fmt.Errorf("updating postgresql.conf: %w", err)
	}

	return &idl.UpdatePgConfRespoonse{}, nil
}

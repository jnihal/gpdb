package agent

import (
	"context"
	"fmt"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

func (s *Server) UpdatePgHbaConf(ctx context.Context, req *idl.UpdatePgHbaConfRequest) (*idl.UpdatePgHbaConfResponse, error) {
	err := postgres.UpdateSegmentPgHbaConf(req.Pgdata, req.Addrs, req.Replication)
	if err != nil {
		return &idl.UpdatePgHbaConfResponse{}, fmt.Errorf("updating pg_hba.conf: %w", err)
	}

	return &idl.UpdatePgHbaConfResponse{}, nil
}

package agent

import (
	"context"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
)

func (s *Server) GetInterfaceAddrs(ctx context.Context, req *idl.GetInterfaceAddrsRequest) (*idl.GetInterfaceAddrsResponse, error) {
	addrs, err := utils.GetHostAddrsNoLoopback()
	if err != nil {
		return &idl.GetInterfaceAddrsResponse{}, err
	}

	return &idl.GetInterfaceAddrsResponse{Addrs: addrs}, nil
}

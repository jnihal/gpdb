package agent

import (
	"context"
	"fmt"
	"strconv"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

func (s *Server) MakeSegment(ctx context.Context, req *idl.MakeSegmentRequest) (*idl.MakeSegmentReply, error) {
	dataDirectory := req.Segment.GetDataDirectory()
	locale := req.GetLocale()

	initdbOptions := postgres.Initdb{
		PgData: dataDirectory,
		Encoding: req.GetEncoding(),
		LcCollate: locale.GetLcCollate(),
		LcCtype: locale.GetLcCtype(),
		
	}
	out, err := postgres.RunPgCommand(&initdbOptions, s.GpHome)
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("executing initdb: %s, %w", out, err)
	}
	gplog.Info(fmt.Sprintf("%s", req.SegConfig))
	configParams := req.SegConfig
	if configParams == nil {
		configParams = make(map[string]string)
	}
	configParams["port"] = strconv.Itoa(int(req.Segment.GetPort()))
	configParams["listen_addresses"] = "*"
	configParams["log_statement"] = "all"
	configParams["gp_contentid"] = strconv.Itoa(int(req.Segment.GetContentid()))
	gplog.Info(fmt.Sprintf("%s", configParams))
	err = postgres.UpdatePostgresqlConf(dataDirectory, configParams, false)
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("updating postgresql.conf: %w", err)
	}

	err = postgres.CreatePostgresInternalConf(dataDirectory, int(req.Segment.GetDbid()))
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("creating internal.auto.conf: %w", err)
	}

	err = postgres.CreatePgHbaConf(dataDirectory, req.GetHbaHostNames(), req.GetIPList(), req.Segment.GetHostName())
	if err != nil {
		return &idl.MakeSegmentReply{}, fmt.Errorf("updating pg_hba.conf: %w", err)
	}

	return &idl.MakeSegmentReply{}, nil
}

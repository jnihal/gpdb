package gpservice_mgmt

import (
	"github.com/greenplum-db/gpdb/gpservice/internal/cli"
	"github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
)

func StartServices(conf *gpservice_config.Config) error {
	return cli.StartServices(conf)
}

func StopServices(conf *gpservice_config.Config) error {
	return cli.StopServices(conf)
}

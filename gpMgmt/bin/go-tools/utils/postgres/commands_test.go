package postgres_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gp/utils/postgres"
)

func TestInitdb(t *testing.T) {
	testhelper.SetupTestLogger()
	
	t.Run("returns the correct command", func(t *testing.T) {
		initdbOptions := postgres.Initdb{
			PgData: "/path/to/pgdata",
			MaxConnections: 10,
		}
	
		initdbCmd := initdbOptions.GetCmd()
		
		t.Logf(initdbCmd.String())
	})
}
package start

import (
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"github.com/greenplum-db/gpdb/gp/utils"
	"os"
	"testing"
)

var (
	p          = utils.GetPlatform()
	configCopy = "config_copy.conf"
)

func TestMain(m *testing.M) {
	exitVal := m.Run()
	tearDownTest()

	os.Exit(exitVal)
}

func tearDownTest() {
	testutils.CleanupFiles(configCopy, testutils.Hostfile)
	testutils.DisableandDeleteServiceFiles(p)
}

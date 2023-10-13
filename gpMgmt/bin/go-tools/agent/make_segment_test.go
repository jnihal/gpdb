package agent_test

import (
	"os"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gp/testutils/exectest"
	"github.com/greenplum-db/gpdb/gp/utils"
)

func init() {
	exectest.RegisterMains(
		InitdbFailure,
	)
}

// Enable exectest.NewCommand mocking.
func TestMain(m *testing.M) {
	os.Exit(exectest.Run(m))
}

func TestMakeSegment(t *testing.T) {
	testhelper.SetupTestLogger()
	
	t.Run("test", func(t *testing.T) {
		utils.System.ExecCommand = exectest.NewCommand(InitdbFailure)
		defer utils.ResetSystemFunctions()
	})
}

func InitdbFailure() {
	os.Stdout.WriteString("error")
	os.Exit(1)
}
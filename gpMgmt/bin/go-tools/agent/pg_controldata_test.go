package agent_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gp/agent"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/testutils/exectest"
	"github.com/greenplum-db/gpdb/gp/utils"
)

func init() {
	exectest.RegisterMains(
		DummyCommand,
	)
}

func TestPgControldata(t *testing.T) {
	testhelper.SetupTestLogger()

	agentServer := agent.New(agent.Config{
		GpHome: "gpHome",
	})

	request := &idl.PgControlDataRequest{
		Pgdata: "gpseg",
	}

	t.Run("succesfully returns the pg_controldata as a map", func(t *testing.T) {
		var pgControlDataCalled bool
		utils.System.ExecCommand = exectest.NewCommandWithVerifier(DummyCommand, func(utility string, args ...string) {
			pgControlDataCalled = true

			expectedUtility := "gpHome/bin/pg_controldata"
			if utility != expectedUtility {
				t.Fatalf("got %s, want %s", utility, expectedUtility)
			}

			expectedArgs := []string{"--pgdata", "gpseg"}
			if !reflect.DeepEqual(args, expectedArgs) {
				t.Fatalf("got %+v, want %+v", args, expectedArgs)
			}
		})
		defer utils.ResetSystemFunctions()

		result, err := agentServer.PgControlData(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !pgControlDataCalled {
			t.Fatalf("expected pg_controldata to be called")
		}

		expected := map[string]string{
			"Database cluster state":            "in production",
			"pg_control last modified":          "Mon Mar 25 14:27:14 2024",
			"Latest checkpoint location":        "0/C08FCE8",
			"Latest checkpoint's REDO location": "0/C08FCB0",
			"Latest checkpoint's REDO WAL file": "000000010000000000000003",
		}
		if !reflect.DeepEqual(result.Result, expected) {
			t.Fatalf("got %+v, want %+v", result.Result, expected)
		}
	})

	t.Run("returns appropriate error when it fails", func(t *testing.T) {
		utils.System.ExecCommand = exectest.NewCommand(exectest.Failure)
		defer utils.ResetSystemFunctions()

		expectedErrPrefix := "executing pg_controldata:"
		_, err := agentServer.PgControlData(context.Background(), request)
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %v", err, expectedErrPrefix)
		}

		var expectedErr *exec.ExitError
		if !errors.As(err, &expectedErr) {
			t.Errorf("got %T, want %T", err, expectedErr)
		}
	})
}

func DummyCommand() {
	os.Stdout.WriteString(`Database cluster state:               in production
pg_control last modified:             Mon Mar 25 14:27:14 2024
Latest checkpoint location:           0/C08FCE8
Latest checkpoint's REDO location:    0/C08FCB0
Latest checkpoint's REDO WAL file:    000000010000000000000003
Invalid entry`)

	os.Exit(0)
}

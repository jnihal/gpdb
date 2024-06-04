package cli_test

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/idl/mock_idl"
	"github.com/greenplum-db/gpdb/gpservice/internal/cli"
	"github.com/greenplum-db/gpdb/gpservice/internal/testutils"
	"github.com/greenplum-db/gpdb/gpservice/internal/testutils/exectest"
	"github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
)

func init() {
	exectest.RegisterMains(
		ServiceStatusOutput,
	)
}

func TestStatusCmd(t *testing.T) {
	t.Run("correctly displays the service status", func(t *testing.T) {
		resetConf := cli.SetConf(testutils.CreateDummyServiceConfig(t))
		defer resetConf()

		utils.System.ExecCommand = exectest.NewCommand(ServiceStatusOutput)
		utils.System.GetHostName = func() (name string, err error) {
			return "cdw", err
		}
		defer utils.ResetSystemFunctions()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		client := mock_idl.NewMockHubClient(ctrl)
		client.EXPECT().StatusAgents(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StatusAgentsReply{
			Statuses: []*idl.ServiceStatus{
				{Role: "Agent", Host: "sdw2", Status: "running", Uptime: "2H", Pid: 456},
				{Role: "Agent", Host: "sdw1", Status: "running", Uptime: "5H", Pid: 123},
			},
		}, nil)
		gpservice_config.SetConnectToHub(client)
		defer gpservice_config.ResetConnectToHub()

		buffer, writer, resetStdout := testutils.CaptureStdout(t)
		defer resetStdout()

		_, err := testutils.ExecuteCobraCommand(t, cli.StatusCmd())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		writer.Close()
		stdout := <-buffer

		expectedStdout := `ROLE      HOST      STATUS    PID       UPTIME
Hub       cdw       running   83008     10H
Agent     sdw2      running   456       2H
Agent     sdw1      running   123       5H
`
		if stdout != expectedStdout {
			t.Fatalf("got %s, want %s", stdout, expectedStdout)
		}
	})
	
	t.Run("errors out when not able to display the hub status", func(t *testing.T) {
		resetConf := cli.SetConf(testutils.CreateDummyServiceConfig(t))
		defer resetConf()

		utils.System.ExecCommand = exectest.NewCommand(exectest.Failure)
		utils.System.GetHostName = func() (name string, err error) {
			return "cdw", err
		}
		defer utils.ResetSystemFunctions()

		buffer, writer, resetStdout := testutils.CaptureStdout(t)
		defer resetStdout()

		_, err := testutils.ExecuteCobraCommand(t, cli.StatusCmd())
		writer.Close()
		stdout := <-buffer
		
		var expectedErr *exec.ExitError
		if !errors.As(err, &expectedErr) {
			t.Errorf("got %T, want %T", err, expectedErr)
		}
		
		expectedErrPrefix := "failed to get service status:"
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}

		expectedStdout := ""
		if stdout != expectedStdout {
			t.Fatalf("got %s, want %s", stdout, expectedStdout)
		}
	})
	
	t.Run("errors out when not able to display the agent status", func(t *testing.T) {
		resetConf := cli.SetConf(testutils.CreateDummyServiceConfig(t))
		defer resetConf()

		utils.System.ExecCommand = exectest.NewCommand(ServiceStatusOutput)
		utils.System.GetHostName = func() (name string, err error) {
			return "cdw", err
		}
		defer utils.ResetSystemFunctions()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errors.New("error")
		client := mock_idl.NewMockHubClient(ctrl)
		client.EXPECT().StatusAgents(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StatusAgentsReply{}, expectedErr)
		gpservice_config.SetConnectToHub(client)
		defer gpservice_config.ResetConnectToHub()

		buffer, writer, resetStdout := testutils.CaptureStdout(t)
		defer resetStdout()

		_, err := testutils.ExecuteCobraCommand(t, cli.StatusCmd())
		writer.Close()
		stdout := <-buffer
		
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}

		expectedStdout := `ROLE      HOST      STATUS    PID       UPTIME
Hub       cdw       running   83008     10H
`
		if stdout != expectedStdout {
			t.Fatalf("got %s, want %s", stdout, expectedStdout)
		}
	})
}

func ServiceStatusOutput() {
	os.Stdout.WriteString(`
ActiveEnterTimestamp=10H
ExecMainStartTimestamp=Sat 2022-09-12 16:31:03 UTC
ExecMainStartTimestampMonotonic=286453245
ExecMainExitTimestampMonotonic=0
ExecMainPID=83001
ExecMainCode=0
ExecMainStatus=0
MainPID=83008
`)
	os.Exit(0)
}

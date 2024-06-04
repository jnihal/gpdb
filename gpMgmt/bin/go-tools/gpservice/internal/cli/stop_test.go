package cli_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/idl/mock_idl"
	"github.com/greenplum-db/gpdb/gpservice/internal/cli"
	"github.com/greenplum-db/gpdb/gpservice/internal/testutils"
	"github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
)

func TestStopCmd(t *testing.T) {
	t.Run("stops only the hub service", func(t *testing.T) {
		_, _, logfile := testhelper.SetupTestLogger()

		resetConf := cli.SetConf(testutils.CreateDummyServiceConfig(t))
		defer resetConf()
		
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		
		client := mock_idl.NewMockHubClient(ctrl)
		client.EXPECT().Stop(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StopHubReply{}, nil)
		gpservice_config.SetConnectToHub(client)
		defer gpservice_config.ResetConnectToHub()

		_, err := testutils.ExecuteCobraCommand(t, cli.StopCmd(), "--hub")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		testutils.AssertLogMessage(t, logfile, `\[INFO\]:-Hub service stopped successfully`)
		testutils.AssertLogMessageNotPresent(t, logfile, `\[INFO\]:-Agent service stopped successfully`)
	})

	t.Run("stops only the agent service", func(t *testing.T) {
		_, _, logfile := testhelper.SetupTestLogger()

		resetConf := cli.SetConf(testutils.CreateDummyServiceConfig(t))
		defer resetConf()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		client := mock_idl.NewMockHubClient(ctrl)
		client.EXPECT().StopAgents(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StopAgentsReply{}, nil)
		gpservice_config.SetConnectToHub(client)
		defer gpservice_config.ResetConnectToHub()

		_, err := testutils.ExecuteCobraCommand(t, cli.StopCmd(), "--agent")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		testutils.AssertLogMessage(t, logfile, `\[INFO\]:-Agent service stopped successfully`)
		testutils.AssertLogMessageNotPresent(t, logfile, `\[INFO\]:-Hub service stopped successfully`)
	})

	t.Run("stops both hub and agent", func(t *testing.T) {
		_, _, logfile := testhelper.SetupTestLogger()

		resetConf := cli.SetConf(testutils.CreateDummyServiceConfig(t))
		defer resetConf()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		client := mock_idl.NewMockHubClient(ctrl)
		client.EXPECT().Stop(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StopHubReply{}, nil)
		client.EXPECT().StopAgents(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StopAgentsReply{}, nil)
		gpservice_config.SetConnectToHub(client)
		defer gpservice_config.ResetConnectToHub()

		_, err := testutils.ExecuteCobraCommand(t, cli.StopCmd())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		testutils.AssertLogMessage(t, logfile, `\[INFO\]:-Hub service stopped successfully`)
		testutils.AssertLogMessage(t, logfile, `\[INFO\]:-Agent service stopped successfully`)
	})

	t.Run("returns error when fails to stop the hub service", func(t *testing.T) {
		testhelper.SetupTestLogger()

		resetConf := cli.SetConf(testutils.CreateDummyServiceConfig(t))
		defer resetConf()
		
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		
		expectedErr := errors.New("error")
		client := mock_idl.NewMockHubClient(ctrl)
		client.EXPECT().StopAgents(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StopAgentsReply{}, nil)
		client.EXPECT().Stop(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StopHubReply{}, expectedErr)
		gpservice_config.SetConnectToHub(client)
		defer gpservice_config.ResetConnectToHub()

		_, err := testutils.ExecuteCobraCommand(t, cli.StopCmd())
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}

		expectedErrPrefix := "failed to stop hub service:"
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})

	t.Run("returns error when fails to stop the agent service", func(t *testing.T) {
		testhelper.SetupTestLogger()

		resetConf := cli.SetConf(testutils.CreateDummyServiceConfig(t))
		defer resetConf()
		
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		
		expectedErr := errors.New("error")
		client := mock_idl.NewMockHubClient(ctrl)
		client.EXPECT().StopAgents(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StopAgentsReply{}, expectedErr)
		client.EXPECT().Stop(
			gomock.Any(),
			gomock.Any(),
		).Times(0)
		gpservice_config.SetConnectToHub(client)
		defer gpservice_config.ResetConnectToHub()

		_, err := testutils.ExecuteCobraCommand(t, cli.StopCmd())
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}

		expectedErrPrefix := "failed to stop agent service:"
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
}

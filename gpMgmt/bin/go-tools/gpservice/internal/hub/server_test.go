package hub_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/idl/mock_idl"
	"github.com/greenplum-db/gpdb/gpservice/internal/hub"
	"github.com/greenplum-db/gpdb/gpservice/internal/testutils"
	"github.com/greenplum-db/gpdb/gpservice/internal/testutils/exectest"
	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
)

func init() {
	exectest.RegisterMains()
}

// Enable exectest.NewCommand mocking.
func TestMain(m *testing.M) {
	os.Exit(exectest.Run(m))
}

func TestStartServer(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("successfully starts the hub server", func(t *testing.T) {
		hubConfig := testutils.CreateDummyServiceConfig(t)
		hubConfig.Hostnames = []string{"localhost"}
		hubConfig.HubPort = testutils.GetPort(t)
		hubServer := hub.New(hubConfig)

		errChan := make(chan error, 1)
		go func() {
			errChan <- hubServer.Start()
		}()
		defer hubServer.Shutdown()

		select {
		case err := <-errChan:
			if err != nil {
				t.Fatalf("unexpected error: %#v", err)
			}
		case <-time.After(1 * time.Second):
			t.Log("hub server started listening")
		}
	})

	t.Run("fails to start the server if not able to load the credentials", func(t *testing.T) {
		expected := errors.New("error")
		hubConfig := testutils.CreateDummyServiceConfig(t)
		hubConfig.Credentials = &testutils.MockCredentials{
			Err: expected,
		}
		hubServer := hub.New(hubConfig)

		errChan := make(chan error, 1)
		go func() {
			errChan <- hubServer.Start()
		}()
		defer hubServer.Shutdown()

		select {
		case err := <-errChan:
			if !errors.Is(err, expected) {
				t.Fatalf("want %#v, got %#v", expected, err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("failed to raise error if load credential fail")
		}
	})
}

func TestStartAgents(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("successfully starts the agents from hub", func(t *testing.T) {
		hubServer := hub.New(testutils.CreateDummyServiceConfig(t))

		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()

		_, err := hubServer.StartAgents(context.Background(), &idl.StartAgentsRequest{})
		if err != nil {
			t.Fatalf("%v", err)
		}
	})
}

func TestDialAllAgents(t *testing.T) {
	testhelper.SetupTestLogger()
	hubConfig := testutils.CreateDummyServiceConfig(t)

	t.Run("successfully establishes connections to agent hosts", func(t *testing.T) {
		hubServer := hub.New(hubConfig)
		err := hubServer.DialAllAgents()
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		connectedHosts := []string{}
		for _, conn := range hubServer.Conns {
			connectedHosts = append(connectedHosts, conn.Hostname)
		}
		sort.Strings(connectedHosts)

		expectedHosts := hubConfig.Hostnames
		if !reflect.DeepEqual(connectedHosts, expectedHosts) {
			t.Fatalf("got %+v, want %+v", connectedHosts, expectedHosts)
		}
	})
	
	t.Run("errors out when connection to agent hosts fail", func(t *testing.T) {
		hubServer := hub.New(hubConfig)
		err := hubServer.DialAllAgents(grpc.WithCredentialsBundle(insecure.NewBundle()))
		expectedErr := "could not connect to agent on host sdw1:"
		if !strings.HasPrefix(err.Error(), expectedErr) {
			t.Fatalf("got %s, want %s", err.Error(), expectedErr)
		}
	})
}

func TestStatusAgents(t *testing.T) {
	testhelper.SetupTestLogger()
	hubServer := hub.New(testutils.CreateDummyServiceConfig(t))

	t.Run("gets the status from the agent hosts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().Status(
			gomock.Any(),
			&idl.StatusAgentRequest{},
			gomock.Any(),
		).Return(&idl.StatusAgentReply{
			Status: "running",
			Uptime: "5H",
			Pid:    123,
		}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().Status(
			gomock.Any(),
			&idl.StatusAgentRequest{},
			gomock.Any(),
		).Return(&idl.StatusAgentReply{
			Status: "running",
			Uptime: "2H",
			Pid:    456,
		}, nil)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		result, err := hubServer.StatusAgents(context.Background(), &idl.StatusAgentsRequest{})
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		expected := &idl.StatusAgentsReply{
			Statuses: []*idl.ServiceStatus{
				{Role: "Agent", Host: "sdw2", Status: "running", Uptime: "2H", Pid: 456},
				{Role: "Agent", Host: "sdw1", Status: "running", Uptime: "5H", Pid: 123},
			},
		}
		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("got %+v, want %+v", result, expected)
		}
	})

	t.Run("errors out when not able to get the status from one of the hosts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().Status(
			gomock.Any(),
			&idl.StatusAgentRequest{},
			gomock.Any(),
		).Return(&idl.StatusAgentReply{
			Status: "running",
			Uptime: "5H",
			Pid:    123,
		}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().Status(
			gomock.Any(),
			&idl.StatusAgentRequest{},
			gomock.Any(),
		).Return(&idl.StatusAgentReply{
			Status: "running",
			Uptime: "2H",
			Pid:    456,
		}, errors.New("error"))

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		_, err := hubServer.StatusAgents(context.Background(), &idl.StatusAgentsRequest{})
		expectedErr := "failed to get agent status on host sdw2"
		if err == nil || !strings.Contains(err.Error(), expectedErr) {
			t.Fatalf("got %v, want %v", err, expectedErr)
		}
	})
}

func TestStopAgents(t *testing.T) {
	testhelper.SetupTestLogger()
	hubServer := hub.New(testutils.CreateDummyServiceConfig(t))

	t.Run("successfully stops all the agents", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().Stop(
			gomock.Any(),
			&idl.StopAgentRequest{},
			gomock.Any(),
		).Return(&idl.StopAgentReply{}, status.Errorf(codes.Unavailable, ""))

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().Stop(
			gomock.Any(),
			&idl.StopAgentRequest{},
			gomock.Any(),
		).Return(&idl.StopAgentReply{}, status.Errorf(codes.Unavailable, ""))

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		_, err := hubServer.StopAgents(context.Background(), &idl.StopAgentsRequest{})
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("errors out when not able to stop the agents", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().Stop(
			gomock.Any(),
			&idl.StopAgentRequest{},
			gomock.Any(),
		).Return(&idl.StopAgentReply{}, status.Errorf(codes.Unavailable, ""))

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().Stop(
			gomock.Any(),
			&idl.StopAgentRequest{},
			gomock.Any(),
		).Return(&idl.StopAgentReply{}, errors.New("error"))

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		_, err := hubServer.StopAgents(context.Background(), &idl.StopAgentsRequest{})
		expectedErr := "failed to stop agent on host sdw2: error"
		if !strings.Contains(err.Error(), expectedErr) {
			t.Fatalf("got %v, want %v", err, expectedErr)
		}
	})
}

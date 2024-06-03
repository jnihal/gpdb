package cli_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/greenplum-db/gpdb/gpservice/internal/cli"
	"github.com/greenplum-db/gpdb/gpservice/internal/testutils"
)

func TestAgentCmd(t *testing.T) {
	t.Run("agent command starts the agent server", func(t *testing.T) {
		config := testutils.CreateDummyServiceConfig(t)
		config.AgentPort = testutils.GetPort(t)
		resetConf := cli.SetConf(config)
		defer resetConf()

		ctx, cancel := context.WithCancel(context.Background())
		go testutils.ExecuteCobraCommandContext(t, ctx, cli.AgentCmd()) //nolint

		testutils.CheckGRPCServerRunning(t, config.AgentPort)
		cancel()
	})

	t.Run("returns error when fails to start the agent server", func(t *testing.T) {
		port, cleanup := testutils.GetAndListenOnPort(t)
		defer cleanup()

		config := testutils.CreateDummyServiceConfig(t)
		config.AgentPort = port
		resetConf := cli.SetConf(config)
		defer resetConf()

		var expectedErr *net.OpError
		_, err := testutils.ExecuteCobraCommand(t, cli.AgentCmd())
		if !errors.As(err, &expectedErr) {
			t.Fatalf("got %T, want %T", err, err)
		}

		expectedErrPrefix := fmt.Sprintf("could not listen on port %d:", port)
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
}

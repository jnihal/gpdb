package cli_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/greenplum-db/gpdb/gpservice/internal/cli"
	"github.com/greenplum-db/gpdb/gpservice/testutils"
)

func TestHubCmd(t *testing.T) {
	t.Run("hub command starts the hub server", func(t *testing.T) {
		config := testutils.CreateDummyServiceConfig(t)
		config.HubPort = testutils.GetPort(t)
		resetConf := cli.SetConf(config)
		defer resetConf()

		ctx, cancel := context.WithCancel(context.Background())
		go testutils.ExecuteCobraCommandContext(t, ctx, cli.HubCmd()) //nolint

		testutils.CheckGRPCServerRunning(t, config.HubPort)
		cancel()
	})

	t.Run("returns error when fails to start the hub server", func(t *testing.T) {
		port, cleanup := testutils.GetAndListenOnPort(t)
		defer cleanup()

		config := testutils.CreateDummyServiceConfig(t)
		config.HubPort = port
		resetConf := cli.SetConf(config)
		defer resetConf()

		var expectedErr *net.OpError
		_, err := testutils.ExecuteCobraCommand(t, cli.HubCmd())
		if !errors.As(err, &expectedErr) {
			t.Fatalf("got %T, want %T", err, err)
		}

		expectedErrPrefix := fmt.Sprintf("could not listen on port %d:", port)
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
}

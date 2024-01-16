package init_cluster

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/test/testutils"
)

func TestEnvValidation(t *testing.T) {
	t.Run("when the given data directory is not empty", func(t *testing.T) {
		var value cli.Segment
		var ok bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig()

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		err = os.MkdirAll(value.DataDirectory, 0777)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		_, err = os.Create(filepath.Join(value.DataDirectory, "abc.txt"))
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		defer os.RemoveAll(value.DataDirectory)

		result, err := testutils.RunInitCluster(configFile)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}

		expectedOut := fmt.Sprintf("[ERROR]:-host: %s, directory not empty:[%s]", value.Hostname, value.DataDirectory)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when the given port is already in use", func(t *testing.T) {
		var value cli.Segment
		var ok bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig()

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		lis, err := net.Listen("tcp", net.JoinHostPort(value.Address, strconv.Itoa(value.Port)))
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		defer lis.Close()

		result, err := testutils.RunInitCluster(configFile)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}

		expectedOut := fmt.Sprintf("[ERROR]:-host: %s, ports already in use: [%s:%d], check if cluster already running", value.Hostname, value.Address, value.Port)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})
}

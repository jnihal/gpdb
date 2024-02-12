package init_cluster

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
)

func TestEnvValidation(t *testing.T) {
	t.Run("when the given data directory is not empty", func(t *testing.T) {
		var value cli.Segment
		var ok bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", coordinator)
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
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
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
		config := GetDefaultConfig(t)

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
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := fmt.Sprintf("[ERROR]:-host: %s, ports already in use: [%s:%d], check if cluster already running", value.Hostname, value.Address, value.Port)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})
	t.Run("when the initdb does not have appropriate permission", func(t *testing.T) {
		var value cli.Segment
		var ok bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		initdbFilePath := "/usr/local/gpdb/bin/initdb"

		// Open the existing initdb file
		file, err := os.OpenFile(initdbFilePath, os.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		defer func() {
			// Teardown function to change file permissions back to 755
			err := file.Chmod(0755)
			if err != nil {
				t.Fatalf("unexpected error during changing initdb file permission: %#v", err)
			}
			file.Close()
		}()

		// Set insufficient permissions (for example, read-only)
		err = file.Chmod(0444)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := fmt.Sprintf("[ERROR]:-host: %s, file %s does not have execute permissions", value.Hostname, initdbFilePath)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when data directory is not empty and --force is given for gp init command", func(t *testing.T) {
		var value cli.Segment
		var ok bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

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

		commonParentDir := filepath.Dir(value.DataDirectory)
		commonParentPattern := filepath.Join(commonParentDir, "*")

		defer func() {
			dirs, err := filepath.Glob(commonParentPattern)

			if err != nil {
				t.Fatalf("Error listing directories: %#v", err)
				return
			}
			for _, dir := range dirs {
				os.RemoveAll(dir)
			}
		}()

		result, err := testutils.RunInitCluster("--force", configFile)
		expectedOut := fmt.Sprintf("[INFO]:-Cluster initialized successfully")
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})
}

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
	"github.com/greenplum-db/gp-common-go-libs/cluster"
    "github.com/greenplum-db/gp-common-go-libs/dbconn"
	
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

		gpHome := os.Getenv("GPHOME")
		if gpHome == "" {
			t.Fatal("GPHOME environment variable not set")
		}

		initdbFilePath := filepath.Join(gpHome, "bin", "initdb")

		if err := os.Chmod(initdbFilePath, 0444); err != nil {
			t.Fatalf("unexpected error during changing initdb file permission: %#v", err)
		}
		defer func() {
			if err := os.Chmod(initdbFilePath, 0755); err != nil {
				t.Fatalf("unexpected error during changing initdb file permission: %#v", err)
			}
		}()

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

		result, err := testutils.RunInitCluster("--force", configFile)
		

		if err != nil {
			t.Fatalf("Error while intializing cluster: %#v", err)
		}
		expectedOut := "[INFO]:-Cluster initialized successfully"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})



	t.Run("Check if the gp_segment_configuration table has the correct value", func(t *testing.T) {
		var value cli.Segment
		var ok bool
		var valueSeg []cli.Segment
		var okSeg bool
	
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
	
		primarySegs := config.Get("primary-segments-array")
		if valueSeg, okSeg = primarySegs.([]cli.Segment); !okSeg {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}
	
		primarySegs = append([]cli.Segment{value}, valueSeg...)
	
		result, err := testutils.RunInitCluster(configFile)
		fmt.Println(result)
	
		if err != nil {
			t.Fatalf("Error while initializing cluster: %#v", err)
		}
	
		conn := dbconn.NewDBConnFromEnvironment("postgres")
		conn.Connect(1)
		segConfigs, err := cluster.GetSegmentConfiguration(conn, false)
		conn.Close()
	
		var mismatchInfo []string
		for i := 0; i < len(segConfigs); i++ {
			if segConfigs[i].Port != primarySegs.([]cli.Segment)[i].Port {
				mismatchInfo = append(mismatchInfo, fmt.Sprintf("Invalid Port at index %d: %d (Expected: %d)", i, segConfigs[i].Port, primarySegs.([]cli.Segment)[i].Port))
			}
			if segConfigs[i].Hostname != primarySegs.([]cli.Segment)[i].Hostname {
				mismatchInfo = append(mismatchInfo, fmt.Sprintf("Invalid Hostname at index %d: %s (Expected: %s)", i, segConfigs[i].Hostname, primarySegs.([]cli.Segment)[i].Hostname))
			}
			if segConfigs[i].DataDir != primarySegs.([]cli.Segment)[i].DataDirectory {
				mismatchInfo = append(mismatchInfo, fmt.Sprintf("Invalid DataDir at index %d: %s (Expected: %s)", i, segConfigs[i].DataDir, primarySegs.([]cli.Segment)[i].DataDirectory))
			}
		}
	
		if len(mismatchInfo) > 0 {
			arrayAsString := strings.Join(mismatchInfo, ", ")
			t.Fatalf(arrayAsString)
		} else {
			fmt.Println("All segments matched successfully.")
		}
		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("initialize cluster with default config and verify default values used correctly", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)
	
		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	
		result, err := testutils.RunInitCluster(configFile)
		fmt.Println(result)
	
		if err != nil {
			t.Fatalf("Error while initializing cluster: %#v", err)
		}
	
		expectedOut1 := "[INFO]:-Could not find encoding in cluster config, defaulting to UTF-8"
		expectedOut2 := "[INFO]:-COORDINATOR max_connections not set, will set to default value 150"
		expectedOut3 := "[INFO]:-shared_buffers is not set, will set to default value 128000kB"
	
		if !strings.Contains(result.OutputMsg, expectedOut1) &&
			!strings.Contains(result.OutputMsg, expectedOut2) &&
			!strings.Contains(result.OutputMsg, expectedOut3) {
			t.Fatalf("Output does not contain the expected strings.\nExpected:\n1. %q\n2. %q\n3. %q\nGot:\n%q",
				expectedOut1, expectedOut2, expectedOut3, result.OutputMsg)
		}
	
		testutils.AssertPgConfig(t, "max_connections", "150", -1)
		testutils.AssertPgConfig(t, "shared_buffers", "125MB", -1)
		testutils.AssertPgConfig(t, "client_encoding", "UTF8", -1)
	
		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
		


	
}		

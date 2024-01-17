package init_cluster

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/test/testutils"
)

func TestInputFileValidation(t *testing.T) {
	t.Run("cluster creation fails when provided input file doesn't exist", func(t *testing.T) {
		result, err := testutils.RunInitCluster("non_existing_file.json")
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-stat non_existing_file.json: no such file or directory"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when the config file is not provided as an input", func(t *testing.T) {
		result, err := testutils.RunInitCluster()
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-please provide config file for cluster initialization"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when invalid number of arguments are given", func(t *testing.T) {
		result, err := testutils.RunInitCluster("abc", "xyz")
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-more arguments than expected"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("cluster creation fails when provided input file has invalid keys", func(t *testing.T) {
		content := `{
			"invalid_key": "value"
		}
		`
		configFile := testutils.GetTempFile(t, "config.json")
		err := os.WriteFile(configFile, []byte(content), 0644)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "non_existing_file.json: no such file or directory"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("cluster creation fails when provided input file has invalid syntax", func(t *testing.T) {
		content := `{
			$$"key": "value"###
		}
		`
		configFile := testutils.GetTempFile(t, "config.json")
		err := os.WriteFile(configFile, []byte(content), 0644)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-error while reading config file:"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("cluster creation fails when input file doesn't have coordinator details", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		err := UnsetConfigKey("coordinator", configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-No primary segments are provided in input config file"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("cluster creation fails when the host does not have gp services configured", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		err := SetConfigKey(configFile, "coordinator", cli.Segment{Hostname: "invalid"})
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-following hostnames [invalid] do not have gp services configured. Please configure the services."
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("cluster creation fails when input file does not have primary segment details", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		err := UnsetConfigKey("primary-segments-array", configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-No primary segments are provided in input config file"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})
	
	t.Run("when encoding is unsupported", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		err := SetConfigKey(configFile, "encoding", "SQL_ASCII")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-SQL_ASCII is no longer supported as a server encoding"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when same data directory is given for a host", func(t *testing.T) {
		var value []cli.Segment
		var ok bool
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig()
		
		primarySegs := config.Get("primary-segments-array")
		if value, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", value)
		}

		err := SetConfigKey(configFile, "primary-segments-array", []cli.Segment{
			{
				Hostname: value[0].Hostname,
				DataDirectory: "gpseg1",
			},
			{
				Hostname: value[0].Hostname,
				DataDirectory: "gpseg1",
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := fmt.Sprintf("[ERROR]:-duplicate data directory entry gpseg1 found for host %s", value[0].Hostname)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when same port is given for a host address", func(t *testing.T) {
		var value []cli.Segment
		var ok bool
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig()
		
		primarySegs := config.Get("primary-segments-array")
		if value, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", value)
		}

		err := SetConfigKey(configFile, "primary-segments-array", []cli.Segment{
			{
				Hostname: value[0].Hostname,
				Address: value[0].Address,
				Port: 1234,
				DataDirectory: "gpseg1",
			},
			{
				Hostname: value[0].Hostname,
				Address: value[0].Address,
				Port: 1234,
				DataDirectory: "gpseg2",
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := fmt.Sprintf("[ERROR]:-duplicate port entry 1234 found for host %s", value[0].Hostname)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})
}

//func TestInputFileValidationSuccess(t *testing.T) {
//	configFile := "/tmp/config.json"
//	_, _ = testutils.RunConfigure("--ca-certificate", "/tmp/certificates/ca-cert.pem",
//		"--ca-key", "/tmp/certificates/ca-key.pem",
//		"--server-certificate", "/tmp/certificates/server-cert.pem",
//		"--server-key", "/tmp/certificates/server-key.pem",
//		"--hostfile", *hostfile)
//	_, _ = testutils.RunStart("services")
//	time.Sleep(5 * time.Second)
//
//	t.Run("cluster creation with no value for shared_buffers in common config", func(t *testing.T) {
//		viper.SetConfigFile(configFile)
//		viper.Set("common-config", map[string]string{"shared_buffers": ""})
//		_ = viper.WriteConfigAs(configFile)
//		result, err := testutils.RunInitCluster(configFile, "--force")
//
//		expectedOut := "shared_buffers is not set, will set to default value"
//		if err != nil {
//			t.Errorf("\nUnexpected error: %v", err)
//		}
//		if result.ExitCode != 0 {
//			t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
//		}
//		if !strings.Contains(result.OutputMsg, expectedOut) {
//			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
//		}
//		viper.Set("common-config", defaultConfig.CommonConfig)
//	})
//
//	t.Run("cluster creation with no value for common config", func(t *testing.T) {
//		viper.SetConfigFile(configFile)
//		viper.Set("common-config", map[string]string{})
//		_ = viper.WriteConfigAs(configFile)
//		result, err := testutils.RunInitCluster(configFile, "--force")
//
//		expectedOut := "shared_buffers is not set, will set to default value"
//		if err != nil {
//			t.Errorf("\nUnexpected error: %v", err)
//		}
//		if result.ExitCode != 0 {
//			t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
//		}
//		if !strings.Contains(result.OutputMsg, expectedOut) {
//			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
//		}
//		viper.Set("common-config", defaultConfig.CommonConfig)
//	})
//
//	t.Run("cluster creation with no value for encoding", func(t *testing.T) {
//		viper.SetConfigFile(configFile)
//		viper.Set("encoding", "")
//		_ = viper.WriteConfigAs(configFile)
//		result, err := testutils.RunInitCluster(configFile, "--force")
//
//		expectedOut := fmt.Sprintf("Could not find encoding in cluster config, defaulting to %v", constants.DefaultEncoding)
//		if err != nil {
//			t.Errorf("\nUnexpected error: %v", err)
//		}
//		if result.ExitCode != 0 {
//			t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
//		}
//		if !strings.Contains(result.OutputMsg, expectedOut) {
//			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
//		}
//		viper.Set("encoding", defaultConfig.CommonConfig)
//	})
//
//	t.Run("cluster creation with no value for max_connection in coordinator config", func(t *testing.T) {
//		viper.SetConfigFile(configFile)
//		viper.Set("coordinator-config", map[string]string{})
//		_ = viper.WriteConfigAs(configFile)
//		result, err := testutils.RunInitCluster(configFile, "--force")
//
//		expectedOut := "COORDINATOR max_connections not set, will set to default value"
//		if err != nil {
//			t.Errorf("\nUnexpected error: %v", err)
//		}
//		if result.ExitCode != 0 {
//			t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
//		}
//		if !strings.Contains(result.OutputMsg, expectedOut) {
//			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
//		}
//		viper.Set("encoding", defaultConfig.CommonConfig)
//	})
//}

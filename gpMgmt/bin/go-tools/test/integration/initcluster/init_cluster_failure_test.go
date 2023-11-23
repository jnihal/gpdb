package initcluster

import (
	"fmt"
	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/test/testutils"
	"github.com/spf13/viper"
	"os"
	"strings"
	"testing"
	"time"
)

func TestInputFileValidation(t *testing.T) {
	configFile := "/tmp/config.json"
	_, _ = testutils.RunConfigure("--ca-certificate", "/tmp/certificates/ca-cert.pem",
		"--ca-key", "/tmp/certificates/ca-key.pem",
		"--server-certificate", "/tmp/certificates/server-cert.pem",
		"--server-key", "/tmp/certificates/server-key.pem",
		"--hostfile", *hostfile)
	_, _ = testutils.RunStart("services")
	time.Sleep(5 * time.Second)

	t.Run("cluster creation fails when provided input file doesn't exist", func(t *testing.T) {
		result, err := testutils.RunInitCluster("non_existing_file.json")
		fmt.Println(result, err)
		expectedOut := "non_existing_file.json: no such file or directory"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}

	})

	t.Run("cluster creation fails when provided input file has invalid keys", func(t *testing.T) {
		invalidContent := `{
	 "cluster":"GPDB",
	 "encoding":"Unicode",
	 "hostnames":true,
	 "password":"gparray",
	}
	`
		_ = os.WriteFile(configFile, []byte(invalidContent), 0644)
		result, err := testutils.RunInitCluster(configFile)
		fmt.Println(result, err)
		expectedOut := "non_existing_file.json: no such file or directory"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}

	})

	t.Run("cluster creation fails when provided input file has invalid syntax", func(t *testing.T) {
		invalidContent := `{
	 "cluster-name":"GPDB"
	 "encoding":"Unicode"
	 "hba-hostnames":true
	 "su-password":"gparray"
	}
	`
		_ = os.WriteFile(configFile, []byte(invalidContent), 0644)
		result, err := testutils.RunInitCluster(configFile)
		fmt.Println(result, err)
		expectedOut := "invalid syntax"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}

	})

	t.Run("cluster creation fails when input file doesn't have coordinator details", func(t *testing.T) {
		viper.SetConfigFile(configFile)
		viper.Set("coordinator", cli.Segment{})
		_ = viper.WriteConfigAs(configFile)
		result, err := testutils.RunInitCluster(configFile)

		expectedOut := "no coordinator details provided"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}

		viper.Set("coordinator", defaultConfig.Coordinator)

		// this test case result services to stop. restarting the services here until the bug fixes.
		_, _ = testutils.RunStart("services")
	})

	t.Run("cluster creation fails when input file doesn't have coordinator address", func(t *testing.T) {
		viper.SetConfigFile(configFile)
		viper.Set("coordinator.address", "")
		_ = viper.WriteConfigAs(configFile)
		result, err := testutils.RunInitCluster(configFile)

		expectedOut := "no coordinator address provided"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}

		viper.Set("coordinator", defaultConfig.Coordinator)
	})

	t.Run("cluster creation with unsupported encoding", func(t *testing.T) {
		viper.SetConfigFile(configFile)
		viper.Set("encoding", "SQL_ASCII")
		_ = viper.WriteConfigAs(configFile)
		result, err := testutils.RunInitCluster(configFile)

		expectedOut := "SQL_ASCII is no longer supported as a server encoding"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}
		viper.Set("encoding", defaultConfig.Encoding)
	})

	t.Run("cluster creation with invalid encoding", func(t *testing.T) {
		viper.SetConfigFile(configFile)
		viper.Set("encoding", "invalid")
		_ = viper.WriteConfigAs(configFile)
		result, err := testutils.RunInitCluster(configFile)

		expectedOut := "executing initdb: initdb: error: \"invalid\" is not a valid server encoding name"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}
		viper.Set("encoding", defaultConfig.Encoding)
	})

	t.Run("cluster creation with non-boolean value for hba-hostnames", func(t *testing.T) {
		viper.SetConfigFile(configFile)
		viper.Set("hba-hostnames", "true")
		_ = viper.WriteConfigAs(configFile)
		result, err := testutils.RunInitCluster(configFile)

		expectedOut := "invalid value for hba-hostnames"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}
		viper.Set("hba-hostnames", defaultConfig.HbaHostnames)
	})

	t.Run("cluster creation with no primary segment details", func(t *testing.T) {
		viper.SetConfigFile(configFile)
		viper.Set("primary-segments-array", []cli.Segment{})
		_ = viper.WriteConfigAs(configFile)
		result, err := testutils.RunInitCluster(configFile)

		expectedOut := "No primary segments are provided in input config file"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}
		viper.Set("primary-segments-array", defaultConfig.PrimarySegmentsArray)
	})

	t.Run("cluster creation with invalid max_connection in coordinator config", func(t *testing.T) {
		viper.SetConfigFile(configFile)
		viper.Set("coordinator-config", map[string]string{"max_connections": "0"})
		_ = viper.WriteConfigAs(configFile)
		result, err := testutils.RunInitCluster(configFile)

		expectedOut := "COORDINATOR_MAX_CONNECT less than 1"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}
		viper.Set("coordinator-config", defaultConfig.CoordinatorConfig)
	})

	t.Run("cluster creation with invalid max_connection in segment config", func(t *testing.T) {
		viper.SetConfigFile(configFile)
		viper.Set("segment-config", map[string]string{"max_connections": "-1"})
		_ = viper.WriteConfigAs(configFile)
		result, err := testutils.RunInitCluster(configFile)

		expectedOut := "max_connections less than 1"
		if err == nil {
			t.Errorf("\nExpected error, got : %#v", err)
		}
		if result.ExitCode != testutils.ExitCode1 {
			t.Errorf("\nExpected: %v \nGot: %v", testutils.ExitCode1, result.ExitCode)
		}
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
		}
		viper.Set("segment-config", defaultConfig.SegmentConfig)
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

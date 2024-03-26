package init_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"github.com/spf13/viper"
)

func TestInputFileValidation(t *testing.T) {
	t.Run("cluster creation fails when provided input file doesn't exist", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		// Remove the coordinator details from the config
		config.Set("coordinator", nil)
		//UnsetConfigKey(t, configFile, "coordinator", true)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		fmt.Println("all config")
		fmt.Println(config.AllSettings())

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		// Construct the expected error message
		expectedOut := "missing coordinator details"

		// Check if the expected error message is contained in the output message
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
		// result, err := testutils.RunInitCluster("non_existing_file.json")
		// if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
		// 	t.Fatalf("got %v, want exit status 1", err)
		// }

		// expectedOut := "[ERROR]:-stat non_existing_file.json: no such file or directory"
		// if !strings.Contains(result.OutputMsg, expectedOut) {
		// 	t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		// }
	})

	t.Run("when the config file is not provided as an input", func(t *testing.T) {
		result, err := testutils.RunInitCluster()
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-please provide config file for cluster initialization"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when invalid number of arguments are given", func(t *testing.T) {
		result, err := testutils.RunInitCluster("abc", "xyz")
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
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
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := `(?s)\[ERROR\]:-while unmarshaling config file: (.*?) has invalid keys: invalid_key`
		match, err := regexp.MatchString(expectedOut, result.OutputMsg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !match {
			t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
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
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-while reading config file:"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	// TODO: Need to fix the bug in the code
	// t.Run("cluster creation fails when input file doesn't have coordinator details", func(t *testing.T) {
	// 	configFile := testutils.GetTempFile(t, "config.json")
	// 	err := UnsetConfigKey(t, configFile, "coordinator")
	// 	if err != nil {
	// 		t.Fatalf("unexpected error: %#v", err)
	// 	}

	// 	result, err := testutils.RunInitCluster(configFile)
	// 	if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
	// 		t.Fatalf("got %v, want exit status 1", err)
	// 	}

	// 	expectedOut := "[ERROR]:-No primary segments are provided in input config file"
	// 	if !strings.Contains(result.OutputMsg, expectedOut) {
	// 		t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
	// 	}
	// })

	t.Run("cluster creation fails when the host does not have gp services configured", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		SetConfigKey(t, configFile, "coordinator", cli.Segment{Hostname: "invalid"}, true)

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-following hostnames [invalid] do not have gp services configured. Please configure the services"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("cluster creation fails when input file does not have primary segment details", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		UnsetConfigKey(t, configFile, "primary-segments-array", true)

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-no primary segments are provided in input config file"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when encoding is unsupported", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		SetConfigKey(t, configFile, "encoding", "SQL_ASCII", true)

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
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
		config := GetDefaultConfig(t)

		primarySegs := config.Get("primary-segments-array")
		if value, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", value)
		}

		SetConfigKey(t, configFile, "primary-segments-array", []cli.Segment{
			{
				Hostname:      value[0].Hostname,
				DataDirectory: "gpseg1",
			},
			{
				Hostname:      value[0].Hostname,
				DataDirectory: "gpseg1",
			},
		}, true)

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := fmt.Sprintf("invalid port has been provided for segment with hostname %s and data_directory gpseg1", value[0].Hostname)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when same port is given for a host address", func(t *testing.T) {
		var value []cli.Segment
		var ok bool
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		primarySegs := config.Get("primary-segments-array")
		if value, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", value)
		}

		SetConfigKey(t, configFile, "primary-segments-array", []cli.Segment{
			{
				Hostname:      value[0].Hostname,
				Address:       value[0].Address,
				Port:          1234,
				DataDirectory: "gpseg1",
			},
			{
				Hostname:      value[0].Hostname,
				Address:       value[0].Address,
				Port:          1234,
				DataDirectory: "gpseg2",
			},
		}, true)

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := fmt.Sprintf("[ERROR]:-duplicate port entry 1234 found for host %s", value[0].Hostname)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when empty data directory is given for a host", func(t *testing.T) {
		var ok bool
		var valueSeg []cli.Segment

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}

		SetConfigKey(t, configFile, "primary-segments-array", []cli.Segment{
			{
				Hostname:      valueSeg[0].Hostname,
				Address:       valueSeg[0].Address,
				Port:          valueSeg[0].Port,
				DataDirectory: valueSeg[0].DataDirectory,
			},
			{
				Hostname:      valueSeg[0].Hostname,
				Address:       valueSeg[0].Address,
				Port:          valueSeg[0].Port,
				DataDirectory: "",
			},
		}, true)

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}
		//to do: this error message needs to be corrected
		expectedOut := fmt.Sprintf("[ERROR]:-data_directory has not been provided for segment with hostname %s and port %d", valueSeg[0].Hostname, valueSeg[0].Port)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when host address is not provided", func(t *testing.T) {
		var ok bool
		var value cli.Segment
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", coordinator)
		}

		//set coordinator host address as empty
		value.Address = ""
		SetConfigKey(t, configFile, "coordinator", value, true)

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		expectedOut := fmt.Sprintf("[WARNING]:-hostAddress has not been provided, populating it with same as hostName %s for the segment with port %d and data_directory %s", value.Hostname, value.Port, value.DataDirectory)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

	})

	t.Run("when both hostaddress and hostnames are not provided or hostname alone is empty", func(t *testing.T) {
		var ok bool
		var valueSeg []cli.Segment
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}

		SetConfigKey(t, configFile, "primary-segments-array", []cli.Segment{
			{
				Hostname: valueSeg[0].Hostname,
				Address:  valueSeg[0].Address,
			},
			{
				Hostname: "",
				Address:  "",
			},
		}, true)

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		//to do: this error message needs to be corrected
		expectedOut := "[ERROR]:-following hostnames [] do not have gp services configured. Please configure the services"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("when port number is not provided", func(t *testing.T) {
		var ok bool
		var value cli.Segment
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", coordinator)
		}

		//set coordinator port number as  empty
		value.Port = 0
		SetConfigKey(t, configFile, "coordinator", value, true)

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := fmt.Sprintf("[ERROR]:-invalid port has been provided for segment with hostname %s and data_directory %s", value.Hostname, value.DataDirectory)
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	//below cases are related to gpinit expansion support

	t.Run("verify expansion with invalid mirror type", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		config.Set("mirroring-type", "invalid_mirroring_type")

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write config to file: %v", err)
		}

		// uncomment below when u want to see what s coming in json

		configSettings := config.AllSettings()

		// Marshal the settings into JSON format
		jsonConfig, err := json.MarshalIndent(configSettings, "", "  ")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Print the JSON configuration
		fmt.Println("Updated configuration:")
		fmt.Println(string(jsonConfig))

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "mirroring type is invalid"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("verify expansion with invalid input file ", func(t *testing.T) {
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
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		//this error msg needs to be corrected
		expectedOut := `(?s)\[ERROR\]:-while unmarshaling config file: (.*?) has invalid keys: invalid_key`
		match, err := regexp.MatchString(expectedOut, result.OutputMsg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !match {
			t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("validate expansion with no Coordinator Details", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		configSettings := config.AllSettings()
		delete(configSettings, "coordinator")

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write config to file: %v", err)
		}

		//comment below
		jsonConfig, err := json.MarshalIndent(configSettings, "", "  ")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		fmt.Println("Updated new configuration:")
		fmt.Println(string(jsonConfig))

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "missing coordinator details"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("verify expansion with mismatched Number of primary and mirror directories", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		primaryDirs := config.GetStringSlice("primary-data-directories")
		primaryDirs = append(primaryDirs, "/tmp/demo/additionalprimary")
		config.Set("primary-data-directories", primaryDirs)

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write updated config to file: %v", err)
		}

		//comment below lines untill runinit cluster now i have put just to see json
		configSettings := config.AllSettings()

		jsonConfig, err := json.MarshalIndent(configSettings, "", "  ")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		fmt.Println("Updated new configuration:")
		fmt.Println(string(jsonConfig))

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "mismatched number of directories"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("verify expansion with insufficient config info- missing mirroring type", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		configSettings := config.AllSettings()
		delete(configSettings, "mirroring-type")

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write config to file: %v", err)
		}

		//uncommen tbelow line if u want to see the json formed
		// jsonConfig, err := json.MarshalIndent(configSettings, "", "  ")
		// if err != nil {
		// 	t.Fatalf("unexpected error: %v", err)
		// }

		// fmt.Println("Updated new configuration:")
		// fmt.Println(string(jsonConfig))

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "insufficient information in the config file"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("verify expansion with empty primary data directories", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		config.Set("primary-data-directories", []string{})

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write config to file: %v", err)
		}

		// uncomment below when u want to see what s coming in json
		// configSettings := config.AllSettings()

		// // Marshal the settings into JSON format
		// jsonConfig, err := json.MarshalIndent(configSettings, "", "  ")
		// if err != nil {
		// 	t.Fatalf("unexpected error: %v", err)
		// }

		// // Print the JSON configuration
		// fmt.Println("Updated configuration:")
		// fmt.Println(string(jsonConfig))

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "empty primary data directories provided"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("verify expansion with empty mirror data directories", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		config.Set("mirror-data-directories", []string{})

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write config to file: %v", err)
		}

		// uncomment below when u want to see what s coming in json
		configSettings := config.AllSettings()

		// Marshal the settings into JSON format
		jsonConfig, err := json.MarshalIndent(configSettings, "", "  ")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Print the JSON configuration
		fmt.Println("Updated configuration:")
		fmt.Println(string(jsonConfig))

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "empty primary data directories provided"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("verify expansion with overlapping Port Ranges", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		config.Set("primary-base-port", 7000)
		config.Set("mirror-base-port", 7001)

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write config to file: %v", err)
		}

		// uncomment below when u want to see what s coming in json
		// configSettings := config.AllSettings()
		// jsonConfig, err := json.MarshalIndent(configSettings, "", "  ")
		// if err != nil {
		// 	t.Fatalf("unexpected error: %v", err)
		// }
		// fmt.Println("Updated configuration:")
		// fmt.Println(string(jsonConfig))

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "overlapping port ranges provided"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})

	t.Run("verify expansion without mirror support", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		configSettings := config.AllSettings()
		delete(configSettings, "mirror-data-directories")
		delete(configSettings, "mirror-base-port")
		delete(configSettings, "mirroring-type")

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write config to file: %v", err)
		}

		// uncomment below when u want to see what s coming in json

		// Marshal the settings into JSON format
		jsonConfig, err := json.MarshalIndent(configSettings, "", "  ")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Print the JSON configuration
		fmt.Println("Updated configuration:")
		fmt.Println(string(jsonConfig))

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
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

	t.Run("verify expansion adds primary and mirror ports automatically when not specified", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		//not sure the bwlo will set value to string but should we set to empty integer not sure test manually also and see the result
		config.Set("primary-base-port", "")
		config.Set("mirror-base-port", "")

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write config to file: %v", err)
		}

		// uncomment below when u want to see what s coming in json

		// Marshal the settings into JSON format
		configSettings := config.AllSettings()

		jsonConfig, err := json.MarshalIndent(configSettings, "", "  ")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Print the JSON configuration
		fmt.Println("Updated configuration:")
		fmt.Println(string(jsonConfig))

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
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

}

func GetDefaultConfig(t *testing.T, expansion ...bool) *viper.Viper {
	t.Helper()
	fmt.Println(expansion)
	instance := viper.New()
	instance.SetConfigFile("sample_init_config.json")
	instance.SetDefault("common-config", make(map[string]string))
	instance.SetDefault("coordinator-config", make(map[string]string))
	instance.SetDefault("segment-config", make(map[string]string))

	err := instance.ReadInConfig()
	if err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	instance.Set("coordinator", cli.Segment{
		Port:          testutils.DEFAULT_COORDINATOR_PORT,
		Hostname:      hostList[0],
		Address:       hostList[0],
		DataDirectory: coordinatorDatadir,
	})

	if len(expansion) == 1 && expansion[0] {
		primaryDataDirectories := make([]string, 0)
		mirrorDataDirectories := make([]string, 0)

		for _, _ = range hostList {
			for i := 1; i <= 4; i++ {
				primaryDataDirectories = append(primaryDataDirectories, fmt.Sprintf("/tmp/demo/primary%d", i))
				mirrorDataDirectories = append(mirrorDataDirectories, fmt.Sprintf("/tmp/demo/mirror%d", i))
			}
		}

		instance.Set("primary-base-port", testutils.DEFAULT_COORDINATOR_PORT+2)
		instance.Set("primary-data-directories", primaryDataDirectories)
		instance.Set("mirror-base-port", testutils.DEFAULT_COORDINATOR_PORT+1002)
		instance.Set("mirroring-type", "spread")
		instance.Set("mirror-data-directories", mirrorDataDirectories)
		instance.Set("hostlist", hostList)

		configMap := instance.AllSettings()
		delete(configMap, "primary-segments-array")

		//instance.Set("primary-segments-array", nil)
		// if err := instance.WriteConfig(); err != nil {
		// 	t.Fatalf("failed to write config: %v", err)
		// }

		// configMap := instance.AllSettings()
		// delete(configMap, "primary-segments-array")
		// err := instance.WriteConfig() // Writes the configuration back to the original file
		// if err != nil {
		// 	t.Fatalf("failed to write config to file: %v", err)
		// }

	} else {
		segs := []cli.Segment{}
		for i := 1; i < 4; i++ {
			host := hostList[0]
			if len(hostList) != 1 {
				host = hostList[i]
			}

			segs = append(segs, cli.Segment{
				Port:          testutils.DEFAULT_COORDINATOR_PORT + i,
				Hostname:      host,
				Address:       host,
				DataDirectory: filepath.Join("/tmp", "demo", fmt.Sprintf("%d", i-1)),
			})
		}
		instance.Set("primary-segments-array", segs)
	}

	return instance
}

func UnsetConfigKey(t *testing.T, filename string, key string, newfile ...bool) {
	t.Helper()

	var config *viper.Viper
	if len(newfile) == 1 && newfile[0] {
		config = GetDefaultConfig(t)
	} else {
		config = viper.New()
		config.SetConfigFile(filename)
		err := config.ReadInConfig()
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	}

	configMap := config.AllSettings()
	delete(configMap, key)

	encodedConfig, err := json.MarshalIndent(configMap, "", " ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = os.WriteFile(filename, encodedConfig, 0777)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func SetConfigKey(t *testing.T, filename string, key string, value interface{}, newfile ...bool) {
	t.Helper()

	var config *viper.Viper
	if len(newfile) == 1 && newfile[0] {
		config = GetDefaultConfig(t)
	} else {
		config = viper.New()
		config.SetConfigFile(filename)
		err := config.ReadInConfig()
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	}

	config.Set(key, value)
	err := config.WriteConfigAs(filename)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

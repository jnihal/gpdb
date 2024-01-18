package init_cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"github.com/spf13/viper"
)

func TestInputFileValidation(t *testing.T) {
	t.Run("cluster creation fails when provided input file doesn't exist", func(t *testing.T) {
		result, err := testutils.RunInitCluster("non_existing_file.json")
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := "[ERROR]:-stat non_existing_file.json: no such file or directory"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}
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

	// TODO: Need to fix the bug in the code
	// t.Run("cluster creation fails when provided input file has invalid keys", func(t *testing.T) {
	// 	content := `{
	// 		"invalid_key": "value"
	// 	}
	// 	`
	// 	configFile := testutils.GetTempFile(t, "config.json")
	// 	err := os.WriteFile(configFile, []byte(content), 0644)
	// 	if err != nil {
	// 		t.Fatalf("unexpected error: %#v", err)
	// 	}

	// 	result, err := testutils.RunInitCluster(configFile)
	// 	if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
	// 		t.Fatalf("got %v, want exit status 1", err)
	// 	}

	// 	expectedOut := "non_existing_file.json: no such file or directory"
	// 	if !strings.Contains(result.OutputMsg, expectedOut) {
	// 		t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
	// 	}
	// })

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

		expectedOut := "[ERROR]:-following hostnames [invalid] do not have gp services configured. Please configure the services."
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

		expectedOut := "[ERROR]:-No primary segments are provided in input config file"
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

		expectedOut := fmt.Sprintf("[ERROR]:-duplicate data directory entry gpseg1 found for host %s", value[0].Hostname)
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
}

func GetDefaultConfig(t *testing.T) *viper.Viper {
	t.Helper()

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

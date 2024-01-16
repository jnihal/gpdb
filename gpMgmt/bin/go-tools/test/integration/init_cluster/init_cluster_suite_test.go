package init_cluster

import (
	"encoding/json"
	"flag"
	"os"
	"testing"

	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/test/testutils"
	"github.com/spf13/viper"
)

var (
	dataDirectories = []string{"/tmp/demo/0", "/tmp/demo/1", "/tmp/demo/2", "/tmp/demo/3"}
	hostList        []string
	hostfile        = flag.String("hostfile", "", "file containing list of hosts")
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	os.Exit(exitCode)
}

func GetDefaultConfig() *viper.Viper {
	currentHost, _ := os.Hostname()

	var config cli.InitConfig
	instance := viper.New()

	instance.SetConfigFile("sampleConfig.json")
	instance.SetDefault("common-config", make(map[string]string))
	instance.SetDefault("coordinator-config", make(map[string]string))
	instance.SetDefault("segment-config", make(map[string]string))

	_ = instance.ReadInConfig()
	_ = instance.Unmarshal(&config)

	config.Coordinator = cli.Segment{
		Port:          7000,
		Hostname:      currentHost,
		Address:       currentHost,
		DataDirectory: dataDirectories[0],
	}

	config.PrimarySegmentsArray = []cli.Segment{
		{
			Port:          7001,
			Hostname:      currentHost,
			Address:       currentHost,
			DataDirectory: dataDirectories[1],
		},
		{
			Port:          7002,
			Hostname:      currentHost,
			Address:       currentHost,
			DataDirectory: dataDirectories[2],
		},
		{
			Port:          7003,
			Hostname:      currentHost,
			Address:       currentHost,
			DataDirectory: dataDirectories[3],
		},
	}

	instance.Set("coordinator", config.Coordinator)
	instance.Set("primary-segments-array", config.PrimarySegmentsArray)

	hostList = testutils.GetHostListFromFile(*hostfile)

	if len(hostList) > 1 {
		for i := range config.PrimarySegmentsArray {
			config.PrimarySegmentsArray[i].Hostname = hostList[i+1]
			config.PrimarySegmentsArray[i].Address = hostList[i+1]
		}
		instance.Set("primary-segments-array", config.PrimarySegmentsArray)
	}

	return instance
}

func UnsetConfigKey(key string, filename string) error {
	config := GetDefaultConfig()

	configMap := config.AllSettings()
	delete(configMap, key)
	encodedConfig, err := json.MarshalIndent(configMap, "", " ")
	if err != nil {
		return err
	}

	err = os.WriteFile(filename, encodedConfig, 0777)
	if err != nil {
		return err
	}

	return nil
}

func SetConfigKey(filename string, key string, value interface{}) error {
	config := GetDefaultConfig()

	config.Set(key, value)
	err := config.WriteConfigAs(filename)
	if err != nil {
		return err
	}

	return nil
}

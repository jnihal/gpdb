package initcluster

import (
	"flag"
	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/test/testutils"
	"github.com/spf13/viper"
	"os"
	"testing"
)

var (
	currentHost     string
	defaultConfig   cli.InitConfig
	dataDirRoot     = "/tmp/demo"
	dataDirectories = []string{"/tmp/demo/0", "/tmp/demo/1", "/tmp/demo/2", "/tmp/demo/3"}
	hostfile        = flag.String("hostfile", "", "file containing list of hosts")
	hostList        []string
)

func init() {
	currentHost, _ = os.Hostname()
	viper.SetConfigFile("sampleConfig.json")
	viper.SetConfigType("json")
	viper.SetDefault("common-config", make(map[string]string))
	viper.SetDefault("coordinator-config", make(map[string]string))
	viper.SetDefault("segment-config", make(map[string]string))

	_ = viper.ReadInConfig()
	_ = viper.Unmarshal(&defaultConfig)

	defaultConfig.Coordinator = cli.Segment{
		Port:          7000,
		Hostname:      currentHost,
		Address:       currentHost,
		DataDirectory: dataDirectories[0],
	}
	defaultConfig.PrimarySegmentsArray = []cli.Segment{
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
	viper.Set("coordinator", defaultConfig.Coordinator)
	viper.Set("primary-segments-array", defaultConfig.PrimarySegmentsArray)
}
func TestMain(m *testing.M) {
	flag.Parse()
	// if hostfile is not provided as input argument, create it with default host
	if *hostfile == "" {
		*hostfile = testutils.DefaultHostfile
		_ = os.WriteFile(*hostfile, []byte(currentHost), 0644)

	} else {
		hostList = testutils.GetHostListFromFile(*hostfile)
		for i, _ := range defaultConfig.PrimarySegmentsArray {
			defaultConfig.PrimarySegmentsArray[i].Hostname = hostList[i+1]
			defaultConfig.PrimarySegmentsArray[i].Address = hostList[i+1]
		}
		viper.Set("primary-segments-array", defaultConfig.PrimarySegmentsArray)
	}
	exitCode := m.Run()
	os.Exit(exitCode)
}

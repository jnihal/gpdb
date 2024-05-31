package gpservice_config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/pkg/greenplum"
	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
	"google.golang.org/grpc"
)

var ConnectToHub = connectToHubFunc

type Config struct {
	HubPort     int      `json:"hubPort"`
	AgentPort   int      `json:"agentPort"`
	Hostnames   []string `json:"hostnames"`
	LogDir      string   `json:"hubLogDir"`
	ServiceName string   `json:"serviceName"`
	GpHome      string   `json:"gphome"`

	Credentials utils.Credentials
}

func (conf *Config) Write(filepath string) error {
	file, err := utils.System.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("could not create service config file %s: %w", filepath, err)
	}
	defer file.Close()

	contents, err := json.MarshalIndent(conf, "", "")
	if err != nil {
		return fmt.Errorf("could not create service config file %s: %w", filepath, err)
	}

	_, err = file.Write(contents)
	if err != nil {
		return fmt.Errorf("could not write to service config file %s: %w", filepath, err)
	}

	err = copyConfigFileToAgents(conf.Hostnames, filepath, conf.GpHome)
	if err != nil {
		return err
	}

	return nil
}

func Create(filepath string, hubPort, agentPort int, hostnames []string, logdir, serviceName, gphome string, creds utils.Credentials) error {
	conf := &Config{
		HubPort:     hubPort,
		AgentPort:   agentPort,
		Hostnames:   hostnames,
		LogDir:      logdir,
		ServiceName: serviceName,
		GpHome:      gphome,
		Credentials: creds,
	}

	return conf.Write(filepath)
}

func Read(filepath string) (*Config, error) {
	config := &Config{}
	config.Credentials = &utils.GpCredentials{}

	contents, err := utils.System.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("could not open service config file %s: %w", filepath, err)
	}

	err = json.Unmarshal(contents, &config)
	if err != nil {
		return nil, fmt.Errorf("could not parse service config file %s: %w", filepath, err)
	}

	return config, nil
}

func copyConfigFileToAgents(hostnames []string, filepath, gpHome string) error {
	gpsyncCmd := &greenplum.GpSync{
		Hostnames:   hostnames,
		Source:      filepath,
		Destination: filepath,
	}

	out, err := utils.RunGpSourcedCommand(gpsyncCmd, gpHome)
	if err != nil {
		return fmt.Errorf("could not copy %s to segment hosts: %s, %w", filepath, out, err)
	}

	return nil
}

func connectToHubFunc(conf *Config) (idl.HubClient, error) {
	credentials, err := conf.Credentials.LoadClientCredentials()
	if err != nil {
		return nil, err
	}

	address := net.JoinHostPort("localhost", strconv.Itoa(conf.HubPort))
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials))
	if err != nil {
		return nil, fmt.Errorf("could not connect to hub on port %d: %w", conf.HubPort, err)
	}

	return idl.NewHubClient(conn), nil
}

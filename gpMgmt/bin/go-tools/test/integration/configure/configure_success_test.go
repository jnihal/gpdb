package configure

import (
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"github.com/greenplum-db/gpdb/gp/utils"
	"os"
	"strings"
	"testing"
)

type ConfigureSuccessTC struct {
	name             string
	cliParams        []string
	logFile          string
	expectedOut      []string
	serviceDir       string
	configFile       string
	skipSvcFileCheck bool
	verifyConfig     func(config hub.Config) hub.Config
	IsSingleNode     bool
	IsMultiNode      bool
}

// positive test cases
var ConfigureSuccessTestcases = []ConfigureSuccessTC{
	{
		name: "configure service with comma separated hosts in --host option",
		cliParams: []string{
			"--host", "cdw",
			"--host", "sdw1",
			"--host", "sdw2",
			"--host", "sdw3",
		},
		expectedOut: successOutput,
		configFile:  testutils.DefaultConfigurationFile,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			return testgpConf
		},
		IsMultiNode: true,
	},
	{
		name: "configure service with --host option",
		cliParams: []string{
			"--host", testutils.DefaultHost,
		},
		configFile: testutils.DefaultConfigurationFile,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			return testgpConf
		},
		expectedOut:  successOutput,
		IsSingleNode: true,
	},
	{
		name:             "service configure shows help with --help",
		cliParams:        []string{"--help"},
		expectedOut:      helpTxt,
		skipSvcFileCheck: true,
		IsMultiNode:      true,
		IsSingleNode:     true,
	},
	{
		name:             "service configure shows help with -h",
		cliParams:        []string{"-h"},
		expectedOut:      helpTxt,
		skipSvcFileCheck: true,
		IsMultiNode:      true,
		IsSingleNode:     true,
	},
	{
		name: "configure service with --hostfile option",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
		},
		expectedOut: successOutput,
		configFile:  testutils.DefaultConfigurationFile,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with host and agent_port option",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--agent-port", "8001"},
		expectedOut: successOutput,
		configFile:  testutils.DefaultConfigurationFile,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			testgpConf.AgentPort = 8001
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with host and hub_port option",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--hub-port", "8001"},
		expectedOut: successOutput,
		configFile:  testutils.DefaultConfigurationFile,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			testgpConf.Port = 8001
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with --service-user option",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--service-user", os.Getenv("USER")},
		expectedOut: successOutput,
		configFile:  testutils.DefaultConfigurationFile,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with server and client certificates",
		cliParams: []string{
			"--ca-certificate", "/tmp/certificates/ca-cert.pem",
			"--ca-key", "/tmp/certificates/ca-key.pem",
			"--server-certificate", "/tmp/certificates/server-cert.pem",
			"--server-key", "/tmp/certificates/server-key.pem",
			"--hostfile", testutils.Hostfile,
		},
		configFile:  testutils.DefaultConfigurationFile,
		expectedOut: successOutput,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			cred := &utils.GpCredentials{
				CACertPath:     "/tmp/certificates/ca-cert.pem",
				CAKeyPath:      "/tmp/certificates/ca-key.pem",
				ServerCertPath: "/tmp/certificates/server-cert.pem",
				ServerKeyPath:  "/tmp/certificates/server-key.pem",
			}
			testgpConf.Credentials = cred
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with verbose option",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--verbose",
		},
		configFile:  testutils.DefaultConfigurationFile,
		expectedOut: successOutput,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with config-file option",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--config-file", "/tmp/gp.conf",
		},
		configFile:  "/tmp/gp.conf",
		expectedOut: successOutput,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with changing gphome value",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--gphome", os.Getenv("GPHOME"),
		},
		configFile:  testutils.DefaultConfigurationFile,
		expectedOut: successOutput,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with log_dir option",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--log-dir", "/tmp",
		},
		logFile:     "/tmp/gp_configure.log",
		configFile:  testutils.DefaultConfigurationFile,
		expectedOut: successOutput,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			testgpConf.LogDir = "/tmp"
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with service-dir option",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--service-dir", "/tmp",
		},
		configFile:  testutils.DefaultConfigurationFile,
		expectedOut: successOutput,
		serviceDir:  "/tmp",
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure create service directory if directory given in service-dir option doesn't exist",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--service-dir", "/tmp/ServiceDir",
		},
		configFile:  testutils.DefaultConfigurationFile,
		expectedOut: successOutput,
		serviceDir:  "/tmp/ServiceDir",
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "configure service with service-name option",
		cliParams: []string{
			"--hostfile", testutils.Hostfile,
			"--service-name", "dummySvc",
		},
		configFile:  testutils.DefaultConfigurationFile,
		expectedOut: successOutput,
		verifyConfig: func(testgpConf hub.Config) hub.Config {
			testgpConf.ServiceName = "dummySvc"
			return testgpConf
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
}

func TestSingleHostConfigureSuccess(t *testing.T) {
	host := testutils.DefaultHost
	testutils.CreateHostfile([]byte(host))

	for _, tc := range ConfigureSuccessTestcases {
		if tc.IsSingleNode {
			t.Run(tc.name, func(t *testing.T) {
				runSuccessTestcases(t, tc)
				config := testutils.ParseConfig(tc.configFile)
				if tc.verifyConfig != nil {
					// check for configuration changes
					testConfig := defaultGPConf
					testConfig.Hostnames = []string{host}
					testutils.EqualValues(t, config, tc.verifyConfig(testConfig))
				}
				if !tc.skipSvcFileCheck {
					// check if service files are created
					tc.serviceDir = testutils.SetDefault(tc.serviceDir, defaultServiceDir)
					agentFile := testutils.GenerateFilePath(tc.serviceDir, config.ServiceName, serviceExt, "agent")
					hubFile := testutils.GenerateFilePath(tc.serviceDir, config.ServiceName, serviceExt, "hub")
					testutils.ServiceFilesExist(t, tc.configFile, tc.logFile, agentFile, hubFile)

					// clean up files after each test cases
					testutils.CleanupFiles(tc.configFile, tc.logFile, agentFile, hubFile)
				}
			})
		}
	}
}

func TestMultiHostConfigureSuccess(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.MultiHosts))

	for _, tc := range ConfigureSuccessTestcases {
		if tc.IsMultiNode {
			t.Run(tc.name, func(t *testing.T) {
				runSuccessTestcases(t, tc)
				config := testutils.ParseConfig(tc.configFile)
				if tc.verifyConfig != nil {
					// check for configuration changes
					testConfig := defaultGPConf
					testConfig.Hostnames = strings.Split(testutils.MultiHosts, "\n")
					testutils.EqualValues(t, tc.verifyConfig(testConfig), config)
				}
				if !tc.skipSvcFileCheck {
					// check if service files are created
					tc.serviceDir = testutils.SetDefault(tc.serviceDir, defaultServiceDir)
					agentFile := testutils.GenerateFilePath(tc.serviceDir, config.ServiceName, serviceExt, "agent")
					hubFile := testutils.GenerateFilePath(tc.serviceDir, config.ServiceName, serviceExt, "hub")
					testutils.ServiceFilesExist(t, agentFile, hubFile)
					testutils.SvcFilesExistsOnRemoteHosts(t, agentFile, strings.Split(testutils.MultiHosts, "\n")[1:])

					// clean up files after each test cases
					testutils.CleanupFiles(tc.configFile, tc.logFile, agentFile, hubFile)
					testutils.CleanupSvcFilesOnRemoteHosts(agentFile, strings.Split(testutils.MultiHosts, "\n")[1:])
				}
			})
		}
	}
}

func runSuccessTestcases(t *testing.T, tc ConfigureSuccessTC) {
	// Running the gp configure command
	out, rc, err := testutils.RunConfigure(tc.cliParams...)
	// check for command result
	testutils.Equal(t, nil, err)
	testutils.Equal(t, 0, rc)
	testutils.Contains(t, tc.expectedOut, out)

	// check if log file is created
	tc.logFile = testutils.SetDefault(tc.logFile, defaultLogFile)
	testutils.FileExists(t, tc.logFile)
}

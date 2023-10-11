package stop

import (
	"os"
	"testing"

	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
)

type StopFailTC struct {
	name             string
	cliParams        []string
	expectedOut      []string
	expectedExitcode int
	additionalSetup  func()
	cleanupFunc      func()
	isSingleNode     bool
	isMultiNode      bool
}

var StopFailTestCases = []StopFailTC{
	{
		name: "stop agents fails when hub is not running",
		cliParams: []string{
			"agents",
		},
		expectedOut: []string{
			"error stopping agent service", "could not connect to hub",
		},
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
			_, _, _ = testutils.RunStop("hub")
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop services fails when services are not running",
		cliParams: []string{
			"services",
		},
		expectedOut: []string{
			"could not connect to hub",
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop hub fails when hub is not running",
		cliParams: []string{
			"hub",
		},
		expectedOut: []string{
			"could not connect to hub",
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop agents fails when services are not running",
		cliParams: []string{
			"agents",
		},
		expectedOut: []string{
			"could not connect to hub",
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop services fails when service configuration file is not present",
		cliParams: []string{
			"services",
		},
		expectedOut: []string{
			"could not open config file", "no such file or directory",
		},
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
			_ = testutils.CopyFile(testutils.DefaultConfigurationFile, "/tmp/config.conf")
			_ = os.RemoveAll(testutils.DefaultConfigurationFile)
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services", "--config-file", "/tmp/config.conf")
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop hub fails when service configuration file is not present",
		cliParams: []string{
			"hub",
		},
		expectedOut: []string{
			"could not open config file", "no such file or directory",
		},
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
			_ = testutils.CopyFile(testutils.DefaultConfigurationFile, "/tmp/config.conf")
			_ = os.RemoveAll(testutils.DefaultConfigurationFile)
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services", "--config-file", "/tmp/config.conf")
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop agents fails when service configuration file is not present",
		cliParams: []string{
			"agents",
		},
		expectedOut: []string{
			"could not open config file", "no such file or directory",
		},
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
			_ = testutils.CopyFile(testutils.DefaultConfigurationFile, "/tmp/config.conf")
			_ = os.RemoveAll(testutils.DefaultConfigurationFile)
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services", "--config-file", "/tmp/config.conf")
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop services fails when certificates are not present",
		cliParams: []string{
			"services", "--config-file", configCopy,
		},
		expectedOut: []string{
			"could not connect to hub",
		},
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
			_ = testutils.CpCfgWithoutCertificates(configCopy)
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop hub fails when certificates are not present",
		cliParams: []string{
			"hub", "--config-file", configCopy,
		},
		expectedOut: []string{
			"could not connect to hub",
		},
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
			_ = testutils.CpCfgWithoutCertificates(configCopy)
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop agents fails when certificates are not present",
		cliParams: []string{
			"agents", "--config-file", configCopy,
		},
		expectedOut: []string{
			"error stopping agent service",
		},
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
			_ = testutils.CpCfgWithoutCertificates(configCopy)
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		expectedExitcode: testutils.ExitCode1,
		isSingleNode:     true,
		isMultiNode:      true,
	},
	{
		name: "stop services with no value for --config-file will fail",
		cliParams: []string{
			"services", "--config-file",
		},
		expectedOut: []string{
			"flag needs an argument: --config-file",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop services with non-existing file for --config-file will fail",
		cliParams: []string{
			"services", "--config-file", "file",
		},
		expectedOut: []string{
			"no such file or directory",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop services with empty string for --config-file will fail",
		cliParams: []string{
			"services", "--config-file", "",
		},
		expectedOut: []string{
			"no such file or directory",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			_, _, _ = testutils.RunStart("services")
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		isSingleNode: true,
		isMultiNode:  true,
	},
}

func TestSingleHostStopFailure(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.DefaultHost))
	for _, tc := range StopFailTestCases {
		if tc.isSingleNode {
			runFailureTestcases(t, tc)
		}
	}
}

func TestMultiHostStopFailure(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.MultiHosts))
	for _, tc := range StopFailTestCases {
		if tc.isMultiNode {
			runFailureTestcases(t, tc)
		}
	}
}

func runFailureTestcases(t *testing.T, tc StopFailTC) {
	t.Run(tc.name, func(t *testing.T) {
		testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		if tc.additionalSetup != nil {
			tc.additionalSetup()
		}
		out, rc, err := testutils.RunStop(tc.cliParams...)
		testutils.NotNil(t, err)
		testutils.Equal(t, tc.expectedExitcode, rc)
		testutils.Contains(t, tc.expectedOut, out)

		if tc.cleanupFunc != nil {
			tc.cleanupFunc()
		}
	})
}

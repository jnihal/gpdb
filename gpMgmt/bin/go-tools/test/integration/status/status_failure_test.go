package status

import (
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"os"
	"testing"
)

type StatusFailTC struct {
	name             string
	cliParams        []string
	expectedOut      []string
	expectedExitcode int
	additionalSetup  func()
	cleanupFunc      func()
	IsSingleNode     bool
	IsMultiNode      bool
}

var StatusFailTestCases = []StatusFailTC{
	{
		name: "checking service status without configuration file will fail",
		cliParams: []string{
			"services",
		},
		expectedOut: []string{
			"could not open config file",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("services")
			_ = testutils.CopyFile(testutils.DefaultConfigurationFile, "/tmp/config.conf")
			_ = os.RemoveAll(testutils.DefaultConfigurationFile)
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services", "--config-file", "/tmp/config.conf")
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "checking status of agents will fail if hub is not running",
		cliParams: []string{
			"agents",
		},
		expectedOut: []string{
			"could not connect to hub",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "checking status of services after stopping hub will fail",
		cliParams: []string{
			"services",
		},
		expectedOut: []string{
			"Hub", "not running", "0",
			"could not connect to hub",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "checking status of agents without certificates",
		cliParams: []string{
			"agents", "--config-file", configCopy,
		},
		expectedOut: []string{
			"error while loading server certificate",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("services")
			_ = testutils.CpCfgWithoutCertificates(configCopy)
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "checking status of services without certificates",
		cliParams: []string{
			"services", "--config-file", configCopy,
		},
		expectedOut: []string{
			"error while loading server certificate",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("services")
			_ = testutils.CpCfgWithoutCertificates(configCopy)
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "checking service status with no value for --config-file will fail",
		cliParams: []string{
			"services", "--config-file",
		},
		expectedOut: []string{
			"flag needs an argument: --config-file",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("services")
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "checking service status with non-existing file for --config-file will fail",
		cliParams: []string{
			"services", "--config-file", "file",
		},
		expectedOut: []string{
			"no such file or directory",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("services")
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "checking service status with empty string for --config-file will fail",
		cliParams: []string{
			"services", "--config-file", "",
		},
		expectedOut: []string{
			"no such file or directory",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("services")
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
}

func TestSingleHostStatusFailures(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.DefaultHost))
	for _, tc := range StatusFailTestCases {
		if tc.IsSingleNode {
			runFailureTestcases(t, tc)
		}
	}
}

func TestMultiHostStatusFailures(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.MultiHosts))
	for _, tc := range StatusFailTestCases {
		if tc.IsMultiNode {
			runFailureTestcases(t, tc)
		}
	}
}

func runFailureTestcases(t *testing.T, tc StatusFailTC) {
	t.Run(tc.name, func(t *testing.T) {
		if tc.additionalSetup != nil {
			tc.additionalSetup()
		}
		out, rc, err := testutils.RunStatus(tc.cliParams...)
		testutils.NotNil(t, err)
		testutils.Equal(t, tc.expectedExitcode, rc)
		testutils.Contains(t, tc.expectedOut, out)

		if tc.cleanupFunc != nil {
			tc.cleanupFunc()
		}
	})
}

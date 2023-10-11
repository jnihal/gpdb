package start

import (
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"os"
	"testing"
)

type StartFailTC struct {
	name             string
	cliParams        []string
	expectedOut      []string
	expectedExitcode int
	additionalSetup  func()
	IsSingleNode     bool
	IsMultiNode      bool
}

var StartFailTestCases = []StartFailTC{
	{
		name: "starting services without configuration file will fail",
		cliParams: []string{
			"services",
		},
		expectedOut: []string{
			"could not open config file",
			"no such file or directory",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			_ = os.RemoveAll(testutils.DefaultConfigurationFile)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "starting agents without starting hub will fail",
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
		IsSingleNode: true,
		IsMultiNode:  true,
	},
	{
		name: "starting hub without service file",
		cliParams: []string{
			"hub",
		},
		expectedOut: []string{
			"failed to start hub service",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			testutils.DisableandDeleteServiceFiles(p)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "starting agents without certificates",
		cliParams: []string{
			"agents", "--config-file", configCopy,
		},
		expectedOut: []string{
			"error while loading server certificate",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_ = testutils.CpCfgWithoutCertificates(configCopy)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "starting services without ca-certificates",
		cliParams: []string{
			"agents",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			_, _, _ = testutils.RunConfigure(append(
				[]string{
					"--hostfile", testutils.Hostfile,
				},
				testutils.CertificateParams[4:]...)...)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "starting services without server-certificates",
		cliParams: []string{
			"agents",
		},
		expectedOut: []string{
			"error while loading server certificate",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			_, _, _ = testutils.RunConfigure(append(
				[]string{
					"--hostfile", testutils.Hostfile,
				},
				testutils.CertificateParams[:4]...)...)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "starting services with no value for --config-file will fail",
		cliParams: []string{
			"services", "--config-file",
		},
		expectedOut: []string{
			"flag needs an argument: --config-file",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "starting services with non-existing file for --config-file will fail",
		cliParams: []string{
			"services", "--config-file", "file",
		},
		expectedOut: []string{
			"no such file or directory",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "starting services with empty string for --config-file will fail",
		cliParams: []string{
			"services", "--config-file", "",
		},
		expectedOut: []string{
			"no such file or directory",
		},
		expectedExitcode: testutils.ExitCode1,
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
}

func TestSingleHostStartFailures(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.DefaultHost))
	for _, tc := range StartFailTestCases {
		if tc.IsSingleNode {
			runFailureTestcases(t, tc)
		}
	}
}

func TestMultiHostStartFailures(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.MultiHosts))
	for _, tc := range StartFailTestCases {
		if tc.IsMultiNode {
			runFailureTestcases(t, tc)
		}
	}
}

func runFailureTestcases(t *testing.T, tc StartFailTC) {
	t.Run(tc.name, func(t *testing.T) {
		if tc.additionalSetup != nil {
			tc.additionalSetup()
		}
		out, rc, err := testutils.RunStart(tc.cliParams...)
		testutils.NotNil(t, err)
		testutils.Equal(t, tc.expectedExitcode, rc)
		testutils.Contains(t, tc.expectedOut, out)
	})
	_, _, _ = testutils.RunStop("services")
}

package configure

import (
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"testing"
)

type ConfigureFailTC struct {
	name             string
	cliParams        []string
	expectedOut      []string
	expectedExitcode int
	IsSigleNode      bool
	IsMultiNode      bool
}

// negative test cases
var ConfigureFailTestCases = []ConfigureFailTC{
	{
		name:      "configure service with empty value for --host option",
		cliParams: []string{"--host", ""},
		expectedOut: []string{
			"please provide a valid input host name",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name:      "configure service with no value for --host option",
		cliParams: []string{"--host"},
		expectedOut: []string{
			"flag needs an argument: --host",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name:      "configure service with empty file for --hostfile option",
		cliParams: []string{"--hostfile", testutils.Hostfile},
		expectedOut: []string{
			"expected at least one host or hostlist specified",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name:      "configure service with no value for --hostfile option",
		cliParams: []string{"--hostfile"},
		expectedOut: []string{
			"flag needs an argument: --hostfile",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name:      "configure service with non-existing host for --host option",
		cliParams: []string{"--host", "host"},
		expectedOut: []string{
			"could not copy gp.conf file to segment hosts",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name:      "configure service with one valid host and invalid host for --host option",
		cliParams: []string{"--host", testutils.DefaultHost, "--host", "invalid"},
		expectedOut: []string{
			"could not copy gp.conf file to segment hosts",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service without any option",
		expectedOut: []string{
			"at least one hostname must be provided using either --host or --hostfile",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name:      "configure service with invalid option",
		cliParams: []string{"--invalid"},
		expectedOut: []string{
			"unknown flag: --invalid",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service with both host and hostfile options",
		cliParams: []string{"--host", testutils.DefaultHost,
			"--hostfile", "abc"},
		expectedOut: []string{
			"[ERROR] if any flags in the group [host hostfile] are set none of the others can be; [host hostfile] were all set",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service with string value for --agent-port option",
		cliParams: []string{"--host", testutils.DefaultHost,
			"--agent-port", "abc"},
		expectedOut: []string{
			"invalid argument",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service with string value for --hub-port option",
		cliParams: []string{"--host", testutils.DefaultHost,
			"--hub-port", "abc"},
		expectedOut: []string{
			"invalid argument",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service with no value for --agent-port option",
		cliParams: []string{"--host", testutils.DefaultHost,
			"--agent-port"},
		expectedOut: []string{
			"flag needs an argument: --agent-port",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service with no value for --hub-port option",
		cliParams: []string{"--host", testutils.DefaultHost,
			"--hub-port"},
		expectedOut: []string{
			"flag needs an argument: --hub-port",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service with non-existing directory as service-dir value",
		cliParams: []string{
			"--host", testutils.DefaultHost,
			"--service-dir", "/newDir/Service-dir",
		},
		expectedOut: []string{
			"could not create service directory /newDir/Service-dir on hosts",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service with no value for service-dir option",
		cliParams: []string{
			"--host", testutils.DefaultHost,
			"--service-dir",
		},
		expectedOut: []string{
			"flag needs an argument: --service-dir",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service with non-existing directory as log-dir value",
		cliParams: []string{
			"--host", testutils.DefaultHost,
			"--log-dir", "/newDir/log_dir",
		},
		expectedOut: []string{
			"no such file or directory",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service with no value for log-dir option",
		cliParams: []string{
			"--host", testutils.DefaultHost,
			"--log-dir",
		},
		// TODO: Add expected output here. Skipped it for now as the test case results in a panic error.
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure fails when value for both --agent-port and --hub-port are same",
		cliParams: []string{
			"--host", testutils.DefaultHost,
			"--agent-port", "2000",
			"--hub-port", "2000",
		},
		// TODO: Add expected output here. Currently this case is not returning error.
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service fails when --gphome value is invalid",
		cliParams: []string{
			"--host", testutils.DefaultHost,
			"--gphome", "invalid",
		},
		expectedOut: []string{
			"could not create configuration file invalid/gp.conf",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service fails when --gphome value is empty",
		cliParams: []string{
			"--host", testutils.DefaultHost,
			"--gphome", "",
		},
		expectedOut: []string{
			"not a valid gphome found",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure service fails when no value given for --gphome",
		cliParams: []string{
			"--host", testutils.DefaultHost,
			"--gphome",
		},
		expectedOut: []string{
			"flag needs an argument: --gphome",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
	{
		name: "configure fails when non-existing service user is given",
		cliParams: []string{
			"--host", testutils.DefaultHost,
			"--service-user", "user"},
		expectedOut: []string{
			"could not create service directory",
		},
		expectedExitcode: testutils.ExitCode1,
		IsMultiNode:      true,
		IsSigleNode:      true,
	},
}

func TestSingleHostConfigureFailure(t *testing.T) {
	testutils.CreateHostfile([]byte(""))
	for _, tc := range ConfigureFailTestCases {
		if tc.IsSigleNode {
			runFailureTestcases(t, tc)
		}
	}
}

func TestMultiHostConfigureFailure(t *testing.T) {
	testutils.CreateHostfile([]byte(""))
	for _, tc := range ConfigureFailTestCases {
		if tc.IsMultiNode {
			runFailureTestcases(t, tc)
		}
	}
}

func runFailureTestcases(t *testing.T, tc ConfigureFailTC) {
	t.Run(tc.name, func(t *testing.T) {
		out, rc, err := testutils.RunConfigure(tc.cliParams...)
		testutils.NotNil(t, err)
		testutils.Equal(t, tc.expectedExitcode, rc)
		testutils.NotContains(t, "panic", out)
		testutils.Contains(t, tc.expectedOut, out)
	})
}

package status

import (
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"strings"
	"testing"
)

var gpCfg hub.Config

type StatusSuccessTC struct {
	name            string
	cliParams       []string
	expectedOut     []string
	serviceName     []string
	additionalSetup func()
	cleanupFunc     func()
	IsSingleNode    bool
	IsMultiNode     bool
}

var StatusSuccessTestCases = []StatusSuccessTC{
	{
		name: "status services shows status of hub and agents",
		cliParams: []string{
			"services",
		},
		expectedOut: []string{
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
			"Hub", "running",
			"Agent", "running",
		},
		serviceName: []string{
			"gp_hub",
			"gp_agent",
		},
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
		name: "status hub shows status of hub when it is not running",
		cliParams: []string{
			"hub",
		},
		expectedOut: []string{
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
			"Hub", "not running",
		},
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status hub shows status of hub",
		cliParams: []string{
			"hub",
		},
		expectedOut: []string{
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
			"Hub", "running",
		},
		serviceName: []string{
			"gp_hub",
		},
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
		name: "status agents shows status of agents",
		cliParams: []string{
			"agents",
		},
		expectedOut: []string{
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
			"Agent", "running",
		},
		serviceName: []string{
			"gp_agent",
		},
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
		name: "status command without params shows help",
		expectedOut: append([]string{
			"Display status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status command with invalid param shows help",
		cliParams: []string{
			"invalid",
		},
		expectedOut: append([]string{
			"Display status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status command with --help shows help",
		cliParams: []string{
			"--help",
		},
		expectedOut: append([]string{
			"Display status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status command with -h shows help",
		cliParams: []string{
			"-h",
		},
		expectedOut: append([]string{
			"Display status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status hub command with --help shows help",
		cliParams: []string{
			"hub", "--help",
		},
		expectedOut: append([]string{
			"Display hub status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status hub command with -h shows help",
		cliParams: []string{
			"hub", "-h",
		},
		expectedOut: append([]string{
			"Display hub status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status agents command with --help shows help",
		cliParams: []string{
			"agents", "--help",
		},
		expectedOut: append([]string{
			"Display agents status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status agents command with -h shows help",
		cliParams: []string{
			"agents", "-h",
		},
		expectedOut: append([]string{
			"Display agents status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status services command with --help shows help",
		cliParams: []string{
			"services", "--help",
		},
		expectedOut: append([]string{
			"Display Hub and Agent services status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status services command with -h shows help",
		cliParams: []string{
			"services", "-h",
		},
		expectedOut: append([]string{
			"Display Hub and Agent services status",
		}, testutils.CommonHelpText...),
		IsMultiNode:  true,
		IsSingleNode: true,
	},
	{
		name: "status services with --verbose cli param",
		cliParams: []string{
			"services", "--verbose",
		},
		expectedOut: []string{
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
			"Hub", "running",
			"Agent", "running",
		},
		serviceName: []string{
			"gp_hub",
			"gp_agent",
		},
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
		name: "status hub with --verbose cli param",
		cliParams: []string{
			"hub", "--verbose",
		},
		expectedOut: []string{
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
			"Hub", "running",
		},
		serviceName: []string{
			"gp_hub",
		},
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
		name: "status agents with --verbose cli param",
		cliParams: []string{
			"agents", "--verbose",
		},
		expectedOut: []string{
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
			"Agent", "running",
		},
		serviceName: []string{
			"gp_agent",
		},
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
		name: "status services when gp installed with --service-name param",
		cliParams: []string{
			"services", "--verbose",
		},
		expectedOut: []string{
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
			"Hub", "running",
			"Agent", "running",
		},
		serviceName: []string{
			"dummySvc_hub",
			"dummySvc_agent",
		},
		additionalSetup: func() {
			params := append(testutils.CertificateParams, []string{"--service-name", "dummySvc"}...)
			testutils.InitService(testutils.Hostfile, params)
			_, _, _ = testutils.RunStart("services")
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		IsMultiNode:  true,
		IsSingleNode: true,
	},
}

func TestSingleHostStatusSuccess(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.DefaultHost))
	for _, tc := range StatusSuccessTestCases {
		if tc.IsSingleNode {
			runSuccessTestcases(t, tc)
		}
	}
}

func TestMultiHostStatusSuccess(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.MultiHosts))
	for _, tc := range StatusSuccessTestCases {
		if tc.IsMultiNode {
			runSuccessTestcases(t, tc)
		}
	}
}

func runSuccessTestcases(t *testing.T, tc StatusSuccessTC) {
	t.Run(tc.name, func(t *testing.T) {
		var listeningPort int
		if tc.additionalSetup != nil {
			tc.additionalSetup()
		}
		gpCfg = testutils.ParseConfig(testutils.DefaultConfigurationFile)

		// Running the gp status command
		out, rc, err := testutils.RunStatus(tc.cliParams...)
		// check for command result
		testutils.Equal(t, nil, err)
		testutils.Equal(t, 0, rc)
		testutils.Contains(t, tc.expectedOut, out)

		// verify the pid in status is listening on correct port
		statusMap := testutils.ExtractStatusData(out)
		var hostPidMap map[string]string
		for _, svc := range tc.serviceName {
			if strings.Contains(svc, "_hub") {
				listeningPort = gpCfg.Port
				hostPidMap = statusMap["Hub"]
			} else if strings.Contains(svc, "_agent") {
				listeningPort = gpCfg.AgentPort
				hostPidMap = statusMap["Agent"]
			}
			for host, pid := range hostPidMap {
				testutils.VerifyServicePIDOnPort(t, pid, listeningPort, host)
			}

		}
		if tc.cleanupFunc != nil {
			tc.cleanupFunc()
		}
	})
}

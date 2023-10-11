package stop

import (
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"github.com/greenplum-db/gpdb/gp/utils"
	"strings"
	"testing"
)

type StopSuccessTC struct {
	name            string
	cliParams       []string
	expectedOut     []string
	serviceName     []string
	additionalSetup func()
	cleanupFunc     func()
	isSingleNode    bool
	isMultiNode     bool
}

var StopSuccessTestCases = []StopSuccessTC{
	{
		name: "stop services successfully",
		cliParams: []string{
			"services",
		},
		expectedOut: []string{
			"Agents stopped successfully",
			"Hub stopped successfully",
		},
		serviceName: []string{
			"gp_hub",
			"gp_agent",
		},
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("services")
		},
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop hub successfully",
		cliParams: []string{
			"hub",
		},
		expectedOut: []string{
			"Hub stopped successfully",
		},
		serviceName: []string{
			"gp_hub",
		},
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("hub")
		},
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop agents successfully",
		cliParams: []string{
			"agents",
		},
		expectedOut: []string{
			"Agents stopped successfully",
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
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop services command with --verbose shows status details",
		cliParams: []string{
			"services", "--verbose",
		},
		expectedOut: []string{
			"Agents stopped successfull",
			"Hub stopped successfully",
		},
		additionalSetup: func() {
			testutils.InitService(testutils.Hostfile, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("services")
		},
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop hub command with --verbose shows status details",
		cliParams: []string{
			"hub", "--verbose",
		},
		expectedOut: []string{
			"Hub stopped successfully",
			"ROLE", "HOST", "STATUS", "PID", "UPTIME",
			"Hub", "not running", "0",
		},
		additionalSetup: func() {
			testutils.InitService(testutils.DefaultHost, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("hub")
		},
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop agents command with --verbose",
		cliParams: []string{
			"agents", "--verbose",
		},
		expectedOut: []string{
			"Agents stopped successfully",
		},
		additionalSetup: func() {
			testutils.InitService(testutils.DefaultHost, testutils.CertificateParams)
			_, _, _ = testutils.RunStart("services")
		},
		cleanupFunc: func() {
			_, _, _ = testutils.RunStop("services")
		},
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop command without params shows help",
		expectedOut: append([]string{
			"Stop processes",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop command with invalid param shows help",
		cliParams: []string{
			"invalid",
		},
		expectedOut: append([]string{
			"Stop processes",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop command with --help shows help",
		cliParams: []string{
			"--help",
		},
		expectedOut: append([]string{
			"Stop processes",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop command with -h shows help",
		cliParams: []string{
			"-h",
		},
		expectedOut: append([]string{
			"Stop processes",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop hub command with --help shows help",
		cliParams: []string{
			"hub", "--help",
		},
		expectedOut: append([]string{
			"Stop hub",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop hub command with -h shows help",
		cliParams: []string{
			"hub", "-h",
		},
		expectedOut: append([]string{
			"Stop hub",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop agents command with --help shows help",
		cliParams: []string{
			"agents", "--help",
		},
		expectedOut: append([]string{
			"Stop agents",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop agents command with -h shows help",
		cliParams: []string{
			"agents", "-h",
		},
		expectedOut: append([]string{
			"Stop agents",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop services command with --help shows help",
		cliParams: []string{
			"services", "--help",
		},
		expectedOut: append([]string{
			"Stop hub and agent services",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
	{
		name: "stop services command with -h shows help",
		cliParams: []string{
			"services", "-h",
		},
		expectedOut: append([]string{
			"Stop hub and agent services",
		}, testutils.CommonHelpText...),
		isSingleNode: true,
		isMultiNode:  true,
	},
}

func TestSingleHostStopSuccess(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.DefaultHost))
	for _, tc := range StopSuccessTestCases {
		if tc.isSingleNode {
			t.Run(tc.name, func(t *testing.T) {
				runSuccessTestcases(t, tc)
				// check if service is not running
				for _, svc := range tc.serviceName {
					status, _, _ := testutils.GetSvcStatusOnHost(p.(utils.GpPlatform), svc, testutils.DefaultHost)
					testutils.VerifySvcNotRunning(t, status)
				}
			})
		}
	}
}

func TestMultiHostStopSuccess(t *testing.T) {
	testutils.CreateHostfile([]byte(testutils.MultiHosts))
	for _, tc := range StopSuccessTestCases {
		if tc.isMultiNode {
			t.Run(tc.name, func(t *testing.T) {
				runSuccessTestcases(t, tc)
				// check if service is not running
				var hostList []string
				for _, svc := range tc.serviceName {
					if strings.Contains(svc, "_hub") {
						hostList = strings.Split(testutils.MultiHosts, "\n")[:1]
					} else if strings.Contains(svc, "_agent") {
						hostList = strings.Split(testutils.MultiHosts, "\n")
					}
					for _, host := range hostList {
						status, _, _ := testutils.GetSvcStatusOnHost(p.(utils.GpPlatform), svc, host)
						testutils.VerifySvcNotRunning(t, status)
					}
				}
				if tc.cleanupFunc != nil {
					tc.cleanupFunc()
				}
			})
		}
	}
}

func runSuccessTestcases(t *testing.T, tc StopSuccessTC) {
	if tc.additionalSetup != nil {
		tc.additionalSetup()
	}
	// Running the gp stop command
	out, rc, err := testutils.RunStop(tc.cliParams...)
	// check for command result
	testutils.Equal(t, nil, err)
	testutils.Equal(t, 0, rc)
	testutils.Contains(t, tc.expectedOut, out)
}

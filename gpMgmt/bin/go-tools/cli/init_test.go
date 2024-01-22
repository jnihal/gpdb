package cli_test

import (
	"errors"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gpdb/gp/idl/mock_idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/spf13/cobra"
	"os"
	"strings"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/testutils"
)

func TestRunInitClusterCmd(t *testing.T) {
	setupTest(t)
	defer teardownTest()
	t.Run("returns error when length of args less than 1", func(t *testing.T) {
		testStr := "please provide config file for cluster initialization"
		cmd := cobra.Command{}
		err := cli.RunInitClusterCmd(&cmd, nil)
		if err == nil || !strings.Contains(err.Error(), testStr) {
			t.Fatalf("got:%v, expected:%s", err, testStr)
		}
	})
	t.Run("returns error when length of args greater than 1", func(t *testing.T) {
		testStr := "more arguments than expected"
		cmd := cobra.Command{}
		args := []string{"/tmp/1", "/tmp/2"}
		err := cli.RunInitClusterCmd(&cmd, args)
		if err == nil || !strings.Contains(err.Error(), testStr) {
			t.Fatalf("got:%v, expected:%s", err, testStr)
		}
	})
	t.Run("returns error cluster creation fails", func(t *testing.T) {
		testStr := "test-error"
		cmd := cobra.Command{}
		args := []string{"/tmp/1"}
		cli.InitClusterService = func(inputConfigFile string, force, verbose bool) error {
			return fmt.Errorf(testStr)
		}
		defer resetCLIVars()
		err := cli.RunInitClusterCmd(&cmd, args)
		if err == nil || !strings.Contains(err.Error(), testStr) {
			t.Fatalf("got:%v, expected:%s", err, testStr)
		}
	})
	t.Run("returns error cluster is created successfully", func(t *testing.T) {

		cmd := cobra.Command{}
		args := []string{"/tmp/1"}
		cli.InitClusterService = func(inputConfigFile string, force, verbose bool) error {
			return nil
		}
		defer resetCLIVars()
		err := cli.RunInitClusterCmd(&cmd, args)
		if err != nil {
			t.Fatalf("got:%v, expected no error", err)
		}
	})
}
func TestInitClusterService(t *testing.T) {
	setupTest(t)
	defer teardownTest()

	t.Run("InitClusterService fails if input config file does not exist", func(t *testing.T) {
		defer resetCLIVars()
		err := cli.InitClusterService("/tmp/invalid_file", false, false)
		if err == nil {
			t.Fatalf("error was expected")
		}
	})

	t.Run("InitClusterService fails if LoadInputConfigToIdl returns error", func(t *testing.T) {
		testStr := "got an error"
		defer resetCLIVars()
		defer utils.ResetSystemFunctions()
		utils.System.Stat = func(name string) (os.FileInfo, error) {
			return nil, nil
		}
		cli.LoadInputConfigToIdl = func(inputConfigFile string, force bool, verbose bool) (*idl.MakeClusterRequest, error) {
			return nil, fmt.Errorf(testStr)
		}
		err := cli.InitClusterService("/tmp/invalid_file", false, false)
		if err == nil || !strings.Contains(err.Error(), testStr) {
			t.Fatalf("got %v, want %s", err, testStr)
		}
	})
	t.Run("InitClusterService fails if ValidateInputConfigAndSetDefaults returns error", func(t *testing.T) {
		testStr := "got an error"
		defer resetCLIVars()
		defer utils.ResetSystemFunctions()
		utils.System.Stat = func(name string) (os.FileInfo, error) {
			return nil, nil
		}
		cli.LoadInputConfigToIdl = func(inputConfigFile string, force bool, verbose bool) (*idl.MakeClusterRequest, error) {
			return nil, nil
		}
		cli.ValidateInputConfigAndSetDefaults = func(request *idl.MakeClusterRequest) error {
			return fmt.Errorf(testStr)
		}
		err := cli.InitClusterService("/tmp/invalid_file", false, false)
		if err == nil || !strings.Contains(err.Error(), testStr) {
			t.Fatalf("got %v, want %s", err, testStr)
		}
	})
	t.Run("InitClusterService fails if ValidateInputConfigAndSetDefaults returns error", func(t *testing.T) {
		testStr := "got an error"
		defer resetCLIVars()
		defer utils.ResetSystemFunctions()
		utils.System.Stat = func(name string) (os.FileInfo, error) {
			return nil, nil
		}
		cli.LoadInputConfigToIdl = func(inputConfigFile string, force bool, verbose bool) (*idl.MakeClusterRequest, error) {
			return nil, nil
		}
		cli.ValidateInputConfigAndSetDefaults = func(request *idl.MakeClusterRequest) error {
			return fmt.Errorf(testStr)
		}
		err := cli.InitClusterService("/tmp/invalid_file", false, false)
		if err == nil || !strings.Contains(err.Error(), testStr) {
			t.Fatalf("got %v, want %s", err, testStr)
		}
	})
	t.Run("returns error if connect to hub fails", func(t *testing.T) {
		testStr := "test-error"
		defer resetCLIVars()
		defer utils.ResetSystemFunctions()
		utils.System.Stat = func(name string) (os.FileInfo, error) {
			return nil, nil
		}
		cli.LoadInputConfigToIdl = func(inputConfigFile string, force bool, verbose bool) (*idl.MakeClusterRequest, error) {
			return nil, nil
		}
		cli.ValidateInputConfigAndSetDefaults = func(request *idl.MakeClusterRequest) error {
			return nil
		}
		cli.ConnectToHub = func(conf *hub.Config) (idl.HubClient, error) {
			return nil, fmt.Errorf(testStr)
		}

		err := cli.InitClusterService("/tmp/invalid_file", false, false)
		if err == nil || !strings.Contains(err.Error(), testStr) {
			t.Fatalf("got %v, want %v", err, testStr)
		}
	})
	t.Run("returns error if RPC returns error", func(t *testing.T) {
		testStr := "test-error"
		defer resetCLIVars()
		defer utils.ResetSystemFunctions()
		utils.System.Stat = func(name string) (os.FileInfo, error) {
			return nil, nil
		}
		cli.LoadInputConfigToIdl = func(inputConfigFile string, force bool, verbose bool) (*idl.MakeClusterRequest, error) {
			return nil, nil
		}
		cli.ValidateInputConfigAndSetDefaults = func(request *idl.MakeClusterRequest) error {
			return nil
		}
		cli.ConnectToHub = func(conf *hub.Config) (idl.HubClient, error) {
			hubClient := mock_idl.NewMockHubClient(ctrl)
			hubClient.EXPECT().MakeCluster(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf(testStr))
			return hubClient, nil
		}

		err := cli.InitClusterService("/tmp/invalid_file", false, false)
		if err == nil || !strings.Contains(err.Error(), testStr) {
			t.Fatalf("got %v, want %v", err, testStr)
		}
	})
	t.Run("returns error if stream receiver returns error", func(t *testing.T) {
		testStr := "test-error"
		defer resetCLIVars()
		defer utils.ResetSystemFunctions()

		utils.System.Stat = func(name string) (os.FileInfo, error) {
			return nil, nil
		}
		cli.LoadInputConfigToIdl = func(inputConfigFile string, force bool, verbose bool) (*idl.MakeClusterRequest, error) {
			return nil, nil
		}
		cli.ValidateInputConfigAndSetDefaults = func(request *idl.MakeClusterRequest) error {
			return nil
		}
		cli.ConnectToHub = func(conf *hub.Config) (idl.HubClient, error) {
			hubClient := mock_idl.NewMockHubClient(ctrl)
			hubClient.EXPECT().MakeCluster(gomock.Any(), gomock.Any()).Return(nil, nil)
			return hubClient, nil
		}
		cli.ParseStreamResponse = func(stream cli.StreamReceiver) error {
			return fmt.Errorf(testStr)
		}
		err := cli.InitClusterService("/tmp/invalid_file", false, false)
		if err == nil || !strings.Contains(err.Error(), testStr) {
			t.Fatalf("got %v, want %v", err, testStr)
		}
	})
}

func resetConfHostnames() {
	cli.Conf.Hostnames = []string{"cdw", "sdw1", "sdw2"}
}

func TestValidateInputConfigAndSetDefaults(t *testing.T) {
	setupTest(t)
	defer teardownTest()
	_, _, logfile := testhelper.SetupTestLogger()
	coordinator := &idl.Segment{
		HostAddress:   "cdw",
		HostName:      "cdw",
		Port:          700,
		DataDirectory: "/tmp/coordinator/",
	}
	gparray := &idl.GpArray{
		Coordinator: coordinator,
		Primaries: []*idl.Segment{
			{
				HostAddress:   "sdw1",
				HostName:      "sdw1",
				Port:          7002,
				DataDirectory: "/tmp/demo/1",
			},
			{
				HostAddress:   "sdw1",
				HostName:      "sdw1",
				Port:          7003,
				DataDirectory: "/tmp/demo/2",
			},
			{
				HostAddress:   "sdw2",
				HostName:      "sdw2",
				Port:          7004,
				DataDirectory: "/tmp/demo/3",
			},
			{
				HostAddress:   "sdw2",
				HostName:      "sdw2",
				Port:          7005,
				DataDirectory: "/tmp/demo/4",
			},
		},
	}
	clusterparamas := &idl.ClusterParams{
		CoordinatorConfig: map[string]string{
			"max_connections": "50",
		},
		SegmentConfig: map[string]string{
			"max_connections":    "150",
			"debug_pretty_print": "off",
			"log_min_messages":   "warning",
		},
		CommonConfig: map[string]string{
			"shared_buffers": "128000kB",
		},
		Locale: &idl.Locale{
			LcAll:      "en_US.UTF-8",
			LcCtype:    "en_US.UTF-8",
			LcTime:     "en_US.UTF-8",
			LcNumeric:  "en_US.UTF-8",
			LcMonetory: "en_US.UTF-8",
			LcMessages: "en_US.UTF-8",
			LcCollate:  "en_US.UTF-8",
		},
		HbaHostnames:  false,
		Encoding:      "Unicode",
		SuPassword:    "gp",
		DbName:        "gpadmin",
		DataChecksums: false,
	}
	var request = &idl.MakeClusterRequest{
		GpArray:       gparray,
		ClusterParams: clusterparamas,
		ForceFlag:     false,
		Verbose:       false,
	}

	t.Run("ValidateInputConfigAndSetDefaults fails if 0 primary segments are provided in input config file", func(t *testing.T) {
		defer resetCLIVars()
		expectedError := "No primary segments are provided in input config file"
		primaries := gparray.Primaries
		gparray.Primaries = []*idl.Segment{}
		err := cli.ValidateInputConfigAndSetDefaults(request)
		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Fatalf("got %v, want %v", err, expectedError)
		}
		gparray.Primaries = primaries
	})

	t.Run("ValidateInputConfigAndSetDefaults fails if some of hosts do not have gp services configured", func(t *testing.T) {
		defer resetCLIVars()
		defer resetConfHostnames()
		cli.Conf.Hostnames = []string{"cdw", "sdw1"}
		expectedError := "following hostnames [sdw2 sdw2] do not have gp services configured. Please configure the services"
		err := cli.ValidateInputConfigAndSetDefaults(request)
		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Fatalf("got: %v, want: %v", err, expectedError)
		}
	})
	t.Run("ValidateInputConfigAndSetDefaults succeeds with info if encoding is not provided", func(t *testing.T) {
		defer resetCLIVars()
		cli.CheckForDuplicatPortAndDataDirectory = func(primaries []*idl.Segment) error {
			return nil
		}
		clusterparamas.Encoding = ""
		cli.Conf.Hostnames = []string{"cdw", "sdw1", "sdw2"}
		err := cli.ValidateInputConfigAndSetDefaults(request)
		if err != nil {
			t.Fatalf("got an unexpected error %v", err)
		}
		expectedLogMsg := `Could not find encoding in cluster config, defaulting to UTF-8`
		testutils.AssertLogMessage(t, logfile, expectedLogMsg)
	})
	t.Run("ValidateInputConfigAndSetDefaults fails if provided encoding is SQL_ASCII", func(t *testing.T) {
		defer resetCLIVars()
		clusterparamas.Encoding = "SQL_ASCII"
		expectedError := "SQL_ASCII is no longer supported as a server encoding"
		err := cli.ValidateInputConfigAndSetDefaults(request)
		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Fatalf("got %v, want %v", err, expectedError)
		}
		clusterparamas.Encoding = "Unicode"
	})
	t.Run("ValidateInputConfigAndSetDefaults succeeds with info if coordinator max_connection is not provided", func(t *testing.T) {
		defer resetCLIVars()
		delete(clusterparamas.CoordinatorConfig, "max_connections")
		err := cli.ValidateInputConfigAndSetDefaults(request)
		if err != nil {
			t.Fatalf("got an unexpected error %v", err)
		}
		expectedLogMsg := `COORDINATOR max_connections not set, will set to default value 150`
		testutils.AssertLogMessage(t, logfile, expectedLogMsg)
		clusterparamas.CoordinatorConfig["max_connections"] = "50"
	})
	t.Run("ValidateInputConfigAndSetDefaults fails if provided coordinator max_connection is less than 1", func(t *testing.T) {
		defer resetCLIVars()
		clusterparamas.CoordinatorConfig["max_connections"] = "-1"
		expectedError := "COORDINATOR_MAX_CONNECT less than 1"
		err := cli.ValidateInputConfigAndSetDefaults(request)
		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Fatalf("got %v, want %v", err, expectedError)
		}
		clusterparamas.CoordinatorConfig["max_connections"] = "50"
	})
	t.Run("ValidateInputConfigAndSetDefaults succeeds with info if shared_buffers are not provided", func(t *testing.T) {
		defer resetCLIVars()
		delete(clusterparamas.CommonConfig, "shared_buffers")
		err := cli.ValidateInputConfigAndSetDefaults(request)
		if err != nil {
			t.Fatalf("got an unexpected error %v", err)
		}
		expectedLogMsg := `shared_buffers is not set, will set to default value 128000kB`
		testutils.AssertLogMessage(t, logfile, expectedLogMsg)
		clusterparamas.CommonConfig["shared_buffers"] = "128000kB"
	})
	t.Run("ValidateInputConfigAndSetDefaults fails if CheckForDuplicatPortAndDataDirectory returns error", func(t *testing.T) {
		defer resetCLIVars()
		expectedError := "Got an error"
		cli.CheckForDuplicatPortAndDataDirectory = func(primaries []*idl.Segment) error {
			return errors.New(expectedError)
		}
		err := cli.ValidateInputConfigAndSetDefaults(request)
		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Fatalf("got %v, want %v", err, expectedError)
		}
	})
}

func TestCheckForDuplicatPortAndDataDirectoryFn(t *testing.T) {
	setupTest(t)
	defer teardownTest()
	var primary0 = &idl.Segment{
		HostAddress:   "sdw1",
		HostName:      "sdw1",
		Port:          7002,
		DataDirectory: "/tmp/demo/1",
	}
	var primary1 = &idl.Segment{
		HostAddress:   "sdw1",
		HostName:      "sdw1",
		Port:          7003,
		DataDirectory: "/tmp/demo/2",
	}
	var primary2 = &idl.Segment{
		HostAddress:   "sdw2",
		HostName:      "sdw2",
		Port:          7004,
		DataDirectory: "/tmp/demo/3",
	}
	var primary3 = &idl.Segment{
		HostAddress:   "sdw2",
		HostName:      "sdw2",
		Port:          7005,
		DataDirectory: "/tmp/demo/4",
	}
	var primaries = []*idl.Segment{
		primary0, primary1, primary2, primary3,
	}

	t.Run("CheckForDuplicatPortAndDataDirectory fails if duplicate data-directory entry is found for a host", func(t *testing.T) {
		defer resetCLIVars()
		expectedError := "duplicate data directory entry /tmp/demo/1 found for host sdw1"
		primary1.DataDirectory = "/tmp/demo/1"
		err := cli.CheckForDuplicatPortAndDataDirectory(primaries)
		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Fatalf("got %v, want %v", err, expectedError)
		}
		primary1.DataDirectory = "/tmp/demo/2"
	})
	t.Run("CheckForDuplicatPortAndDataDirectory fails if duplicate port entry is found for a host", func(t *testing.T) {
		defer resetCLIVars()
		expectedError := "duplicate port entry 7002 found for host sdw1"
		primary1.Port = 7002
		err := cli.CheckForDuplicatPortAndDataDirectory(primaries)
		if err == nil || !strings.Contains(err.Error(), expectedError) {
			t.Fatalf("got %v, want %v", err, expectedError)
		}
		primary1.Port = 7003
	})
	t.Run("CheckForDuplicatPortAndDataDirectory succeeds if no duplicate port/datadir entry is found for any of the hosts", func(t *testing.T) {
		defer resetCLIVars()
		err := cli.CheckForDuplicatPortAndDataDirectory(primaries)
		if err != nil {
			t.Fatalf("got an unexpected error")
		}
	})
}

package platform_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"reflect"
	"strings"
	"testing"

	"github.com/greenplum-db/gpdb/gpservice/constants"
	"github.com/greenplum-db/gpdb/gpservice/internal/platform"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/internal/testutils/exectest"
	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
)

func init() {
	exectest.RegisterMains(
		ServiceStatusOutput,
		ServiceStopped,
	)
}

// Enable exectest.NewCommand mocking.
func TestMain(m *testing.M) {
	os.Exit(exectest.Run(m))
}

func TestCreateServiceDir(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("returns error when not able to create the directory", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.ExecCommand = exectest.NewCommand(exectest.Failure)
		defer utils.ResetSystemFunctions()

		err := platform.CreateServiceDir([]string{"host1"}, "path/to/serviceDir", "gpHome")
		var expectedErr *exec.ExitError
		if !errors.As(err, &expectedErr) {
			t.Errorf("got %T, want %T", err, expectedErr)
		}

		expectedErrPrefix := "could not create service directory path/to/serviceDir on hosts:"
		if !strings.Contains(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})

	t.Run("succesfully creates the directory", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()

		err := platform.CreateServiceDir([]string{"host1"}, "path/to/serviceDir", "gpHome")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestWriteServiceFile(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("errors when could not open the file", func(t *testing.T) {
		file, err := os.CreateTemp("", "test")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		defer os.Remove(file.Name())

		err = os.Chmod(file.Name(), 0000)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		err = platform.WriteServiceFile(file.Name(), "abc")
		if !strings.HasPrefix(err.Error(), "could not create service file") {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("WriteServiceFile successfully writes to a file", func(t *testing.T) {
		file, err := os.CreateTemp("", "test")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		defer os.Remove(file.Name())

		expected := "abc"
		err = platform.WriteServiceFile(file.Name(), expected)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		buf, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatalf("error reading file %q: %v", file.Name(), err)
		}
		contents := string(buf)

		if contents != expected {
			t.Fatalf("got %q, want %q", contents, expected)
		}
	})
}

func TestGenerateServiceFileContents(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("GenerateServiceFileContents successfully generates contents for darwin", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformDarwin)

		expected := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>gpservice_hub</string>
    <key>ProgramArguments</key>
    <array>
        <string>/test/bin/gpservice</string>
        <string>hub</string>
    </array>
    <key>StandardOutPath</key>
    <string>/tmp/grpc_hub.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/grpc_hub.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>%[1]s</string>
        <key>GPHOME</key>
        <string>/test</string>
    </dict>
</dict>
</plist>
`, os.Getenv("PATH"))
		contents := platform.GenerateServiceFileContents("hub", "/test", "gpservice")
		if contents != expected {
			t.Fatalf("got %q, want %q", contents, expected)
		}
	})

	t.Run("GenerateServiceFileContents successfully generates contents for linux", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		expected := `[Unit]
Description=Greenplum Database management utility hub

[Service]
Type=simple
Environment=GPHOME=/test
ExecStart=/test/bin/gpservice hub
Restart=on-failure
StandardOutput=file:/tmp/grpc_hub.log
StandardError=file:/tmp/grpc_hub.log

[Install]
Alias=gpservice_hub.service
WantedBy=default.target
`
		contents := platform.GenerateServiceFileContents("hub", "/test", "gpservice")
		if contents != expected {
			t.Fatalf("got %q, want %q", contents, expected)
		}
	})
}

func TestGetDefaultServiceDir(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("GetDefaultServiceDir returns the correct directory path", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.CurrentUser = func() (*user.User, error) {
			return &user.User{
				Username: "admin",
			}, nil
		}
		defer utils.ResetSystemFunctions()

		expected := "/home/admin/.config/systemd/user"
		result := platform.GetDefaultServiceDir()
		if result != expected {
			t.Fatalf("got %q, want %q", result, expected)
		}

		platform = GetPlatform(t, constants.PlatformDarwin)

		expected = "/Users/admin/Library/LaunchAgents"
		result = platform.GetDefaultServiceDir()
		if result != expected {
			t.Fatalf("got %q, want %q", result, expected)
		}
	})
}

func TestReloadServices(t *testing.T) {
	testhelper.SetupTestLogger()

	type test struct {
		os        string
		service   string
		errSuffix string
	}

	success_tests := []test{
		{os: "darwin", service: "hub"},
		{os: "darwin", service: "agent"},
		{os: constants.PlatformLinux, service: "hub"},
		{os: constants.PlatformLinux, service: "agent"},
	}
	for _, tc := range success_tests {
		t.Run(fmt.Sprintf("reloading of %s service succeeds on %s", tc.service, tc.os), func(t *testing.T) {
			var err error
			platform := GetPlatform(t, tc.os)

			utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
			defer utils.ResetSystemFunctions()

			if tc.service == "hub" {
				err = platform.ReloadHubService("/path/to/service/file")
			} else {
				err = platform.ReloadAgentService("gpHome", []string{"host1"}, "/path/to/service/file")
			}

			if err != nil {
				t.Fatalf("unexpected error: %#v", err)
			}
		})
	}

	failure_tests_darwin := []test{
		{os: "darwin", service: "hub"},
		{os: "darwin", service: "agent", errSuffix: " on segment hosts"},
	}
	for _, tc := range failure_tests_darwin {
		t.Run(fmt.Sprintf("reloading of %s service returns error when not able to unload the file on darwin", tc.service), func(t *testing.T) {
			var err error
			platform := GetPlatform(t, tc.os)

			utils.System.ExecCommand = func(name string, args ...string) *exec.Cmd {
				if strings.Contains(strings.Join(args, " "), "unload") {
					return exectest.NewCommand(exectest.Failure)(name, args...)
				}

				return exectest.NewCommand(exectest.Success)(name, args...)
			}
			defer utils.ResetSystemFunctions()

			if tc.service == "hub" {
				err = platform.ReloadHubService("/path/to/service/file")
			} else {
				err = platform.ReloadAgentService("gpHome", []string{"host1"}, "/path/to/service/file")
			}

			var expectedErr *exec.ExitError
			if !errors.As(err, &expectedErr) {
				t.Errorf("got %T, want %T", err, expectedErr)
			}

			expectedErrPrefix := fmt.Sprintf("could not unload %s service file /path/to/service/file%s:", tc.service, tc.errSuffix)
			if !strings.Contains(err.Error(), expectedErrPrefix) {
				t.Fatalf("got %v, want %s", err, expectedErrPrefix)
			}
		})

		t.Run(fmt.Sprintf("reloading of %s service returns error when not able to load the file on darwin", tc.service), func(t *testing.T) {
			var err error
			platform := GetPlatform(t, constants.PlatformDarwin)

			utils.System.ExecCommand = func(name string, args ...string) *exec.Cmd {
				if strings.Contains(strings.Join(args, " "), "unload") {
					return exectest.NewCommand(exectest.Success)(name, args...)
				}

				return exectest.NewCommand(exectest.Failure)(name, args...)
			}
			defer utils.ResetSystemFunctions()

			if tc.service == "hub" {
				err = platform.ReloadHubService("/path/to/service/file")
			} else {
				err = platform.ReloadAgentService("gpHome", []string{"host1"}, "/path/to/service/file")
			}

			var expectedErr *exec.ExitError
			if !errors.As(err, &expectedErr) {
				t.Errorf("got %T, want %T", err, expectedErr)
			}

			expectedErrPrefix := fmt.Sprintf("could not load %s service file /path/to/service/file%s:", tc.service, tc.errSuffix)
			if !strings.Contains(err.Error(), expectedErrPrefix) {
				t.Fatalf("got %v, want %s", err, expectedErrPrefix)
			}
		})

	}

	failure_tests_linux := []test{
		{os: constants.PlatformLinux, service: "hub"},
		{os: constants.PlatformLinux, service: "agent", errSuffix: " on segment hosts"},
	}
	for _, tc := range failure_tests_linux {
		t.Run(fmt.Sprintf("reloading of %s service returns error when not able to reload the file on linux", tc.service), func(t *testing.T) {
			var err error
			platform := GetPlatform(t, constants.PlatformLinux)

			utils.System.ExecCommand = exectest.NewCommand(exectest.Failure)
			defer utils.ResetSystemFunctions()

			if tc.service == "hub" {
				err = platform.ReloadHubService("/path/to/service/file")
			} else {
				err = platform.ReloadAgentService("gpHome", []string{"host1"}, "/path/to/service/file")
			}

			var expectedErr *exec.ExitError
			if !errors.As(err, &expectedErr) {
				t.Errorf("got %T, want %T", err, expectedErr)
			}

			expectedErrPrefix := fmt.Sprintf("could not reload %s service file /path/to/service/file%s:", tc.service, tc.errSuffix)
			if !strings.Contains(err.Error(), expectedErrPrefix) {
				t.Fatalf("got %v, want %s", err, expectedErrPrefix)
			}
		})
	}
}

func TestCreateAndInstallHubServiceFile(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("CreateAndInstallHubServiceFile runs successfully", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			_, writer, _ := os.Pipe()

			return writer, nil
		}
		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()

		err := platform.CreateAndInstallHubServiceFile("gpHome", "testdir", "gptest")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("CreateAndInstallHubServiceFile errors when not able to write to a file", func(t *testing.T) {
		expectedErr := os.ErrPermission
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return nil, expectedErr
		}
		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()

		err := platform.CreateAndInstallHubServiceFile("gpHome", "testdir", "gptest")
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %q, want %q", err, expectedErr)
		}
	})

	t.Run("CreateAndInstallHubServiceFile errors when not able to reload the service", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			_, writer, _ := os.Pipe()

			return writer, nil
		}
		utils.System.ExecCommand = exectest.NewCommand(exectest.Failure)
		defer utils.ResetSystemFunctions()

		err := platform.CreateAndInstallHubServiceFile("gpHome", "testdir", "gptest")
		var expectedErr *exec.ExitError
		if !errors.As(err, &expectedErr) {
			t.Errorf("got %T, want %T", err, expectedErr)
		}

		expectedErrPrefix := "could not reload hub service file testdir/gptest_hub.service:"
		if !strings.Contains(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
}

func TestCreateAndInstallAgentServiceFile(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("CreateAndInstallAgentServiceFile runs successfully", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			_, writer, _ := os.Pipe()

			return writer, nil
		}
		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()

		err := platform.CreateAndInstallAgentServiceFile([]string{"host1", "host2"}, "gpHome", "testdir", "gptest")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("CreateAndInstallAgentServiceFile errors when gpsync fails", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			_, writer, _ := os.Pipe()

			return writer, nil
		}
		utils.System.ExecCommand = func(name string, args ...string) *exec.Cmd {
			if strings.Contains(strings.Join(args, " "), "gpsync") {
				return exectest.NewCommand(exectest.Failure)(name, args...)
			}

			return exectest.NewCommand(exectest.Success)(name, args...)
		}
		defer utils.ResetSystemFunctions()

		err := platform.CreateAndInstallAgentServiceFile([]string{"host1", "host2"}, "gpHome", "testdir", "gptest")
		var expectedErr *exec.ExitError
		if !errors.As(err, &expectedErr) {
			t.Errorf("got %T, want %T", err, expectedErr)
		}

		expectedErrPrefix := "could not copy agent service file to segment hosts:"
		if !strings.Contains(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})

	t.Run("CreateAndInstallAgentServiceFile errors when not able to write to a file", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return nil, os.ErrPermission
		}
		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()

		err := platform.CreateAndInstallAgentServiceFile([]string{"host1", "host2"}, "gpHome", "testdir", "gptest")
		expectedErr := os.ErrPermission
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %q, want %q", err, expectedErr)
		}
	})

	t.Run("CreateAndInstallAgentServiceFile errors when not able to reload the service", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			_, writer, _ := os.Pipe()

			return writer, nil
		}
		utils.System.ExecCommand = func(name string, args ...string) *exec.Cmd {
			if strings.HasSuffix(name, "gpsync") {
				return exectest.NewCommand(exectest.Success)(name, args...)
			}

			return exectest.NewCommand(exectest.Failure)(name, args...)
		}
		defer utils.ResetSystemFunctions()

		err := platform.CreateAndInstallAgentServiceFile([]string{"host1", "host2"}, "gpHome", "testdir", "gptest")
		var expectedErr *exec.ExitError
		if !errors.As(err, &expectedErr) {
			t.Errorf("got %T, want %T", err, expectedErr)
		}

		expectedErrPrefix := "could not copy agent service file to segment hosts:"
		if !strings.Contains(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
}

func TestGetStartHubCommand(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("GetStartHubCommand returns the correct command for linux", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		result := platform.GetStartHubCommand("gptest").Args
		expected := []string{"systemctl", "--user", "start", "gptest_hub"}
		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("got %+v, want %+v", result, expected)
		}
	})

	t.Run("GetStartHubCommand returns the correct command for darwin", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformDarwin)

		result := platform.GetStartHubCommand("gptest").Args
		expected := []string{"launchctl", "start", "gptest_hub"}
		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("got %+v, want %+v", result, expected)
		}
	})
}

func TestGetStartAgentCommandString(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("GetStartAgentCommandString returns the correct string for linux", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		result := platform.GetStartAgentCommandString("gptest")
		expected := []string{"systemctl", "--user", "start", "gptest_agent"}
		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("got %+v, want %+v", result, expected)
		}
	})

	t.Run("GetStartAgentCommandString returns the correct string for darwin", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformDarwin)

		result := platform.GetStartAgentCommandString("gptest")
		expected := []string{"launchctl", "", "start", "gptest_agent"}
		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("got %+v, want %+v", result, expected)
		}
	})
}

func TestGetServiceStatusMessage(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("GetServiceStatusMessage successfully gets the service status for linux", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.ExecCommand = exectest.NewCommand(ServiceStatusOutput)
		defer utils.ResetSystemFunctions()

		result, _ := platform.GetServiceStatusMessage("gptest")
		expected := "got status of the service"
		if result != expected {
			t.Fatalf("got %q, want %q", result, expected)
		}
	})

	t.Run("GetServiceStatusMessage successfully gets the service status for darwin", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformDarwin)

		utils.System.ExecCommand = exectest.NewCommand(ServiceStatusOutput)
		defer utils.ResetSystemFunctions()

		result, _ := platform.GetServiceStatusMessage("gptest")
		expected := "got status of the service"
		if result != expected {
			t.Fatalf("got %q, want %q", result, expected)
		}
	})

	t.Run("GetServiceStatusMessage does not throw error when service is stopped", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.ExecCommand = exectest.NewCommand(ServiceStopped)
		defer utils.ResetSystemFunctions()

		_, err := platform.GetServiceStatusMessage("gptest")
		if err != nil {
			t.Fatalf("unexpected err: %#v", err)
		}
	})

	t.Run("GetServiceStatusMessage errors when not able to get the service status", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.ExecCommand = exectest.NewCommand(exectest.Failure)
		defer utils.ResetSystemFunctions()

		output, err := platform.GetServiceStatusMessage("gptest")
		if output != "" {
			t.Fatalf("expected empty output, got %q", output)
		}

		expectedErr := "exit status 1"
		if err.Error() != expectedErr {
			t.Fatalf("got %q, want %q", err, expectedErr)
		}
	})
}

func TestParseServiceStatusMessage(t *testing.T) {
	testhelper.SetupTestLogger()

	cases := []struct {
		name     string
		os       string
		message  string
		expected *idl.ServiceStatus
	}{
		{
			name: "ParseServiceStatusMessage gets status for darwin when service is running",
			os:   constants.PlatformDarwin,
			message: `
			{
				"StandardOutPath" = "/tmp/grpc_hub.log";
				"LimitLoadToSessionType" = "Aqua";
				"StandardErrorPath" = "/tmp/grpc_hub.log";
				"Label" = "gp_hub";
				"OnDemand" = true;
				"LastExitStatus" = 0;
				"PID" = 19909;
				"Program" = "/usr/local/gpdb/bin/gp";
				"ProgramArguments" = (
					"/usr/local/gpdb/bin/gp";
					"hub";
				);
			};
			`,
			expected: &idl.ServiceStatus{Status: "running", Pid: uint32(19909)},
		},
		{
			name: "ParseServiceStatusMessage gets status for darwin when service is not running",
			os:   constants.PlatformDarwin,
			message: `
			{
				"StandardOutPath" = "/tmp/grpc_hub.log";
				"LimitLoadToSessionType" = "Aqua";
				"StandardErrorPath" = "/tmp/grpc_hub.log";
				"Label" = "gp_hub";
				"OnDemand" = true;
				"LastExitStatus" = 0;
				"Program" = "/usr/local/gpdb/bin/gp";
				"ProgramArguments" = (
					"/usr/local/gpdb/bin/gp";
					"hub";
				);
			};
			`,
			expected: &idl.ServiceStatus{Status: "not running"},
		},
		{
			name: "ParseServiceStatusMessage gets status for linux when service is running",
			os:   constants.PlatformLinux,
			message: `
			ActiveEnterTimestamp=Sun 2023-08-20 14:43:35 UTC
			ExecMainStartTimestamp=Sat 2022-09-12 16:31:03 UTC
			ExecMainStartTimestampMonotonic=286453245
			ExecMainExitTimestampMonotonic=0
			ExecMainPID=83001
			ExecMainCode=0
			ExecMainStatus=0
			MainPID=83008
			`,
			expected: &idl.ServiceStatus{Status: "running", Uptime: "Sun 2023-08-20 14:43:35 UTC", Pid: uint32(83008)},
		},
		{
			name: "ParseServiceStatusMessage gets status for linux when service is not running",
			os:   constants.PlatformLinux,
			message: `
			ExecMainStartTimestampMonotonic=286453245
			ExecMainExitTimestampMonotonic=0
			ExecMainPID=83001
			ExecMainCode=0
			ExecMainStatus=0
			MainPID=0
			`,
			expected: &idl.ServiceStatus{Status: "not running", Pid: uint32(0)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			platform := GetPlatform(t, constants.PlatformDarwin)

			result := platform.ParseServiceStatusMessage(tc.message)
			if !reflect.DeepEqual(&result, tc.expected) {
				t.Fatalf("got %+v, want %+v", &result, tc.expected)
			}
		})
	}
}

func TestEnableUserLingering(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("EnableUserLingering run successfully for linux", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.ExecCommand = exectest.NewCommand(exectest.Success)
		defer utils.ResetSystemFunctions()

		err := platform.EnableUserLingering([]string{"host1", "host2"}, "/path/to/gpHome", "serviceUser")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("EnableUserLingering runs successfully for other platforms", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformDarwin)

		err := platform.EnableUserLingering([]string{"host1", "host2"}, "path/to/gpHome", "serviceUser")
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("EnableUserLingering returns error on failure", func(t *testing.T) {
		platform := GetPlatform(t, constants.PlatformLinux)

		utils.System.ExecCommand = exectest.NewCommand(exectest.Failure)
		defer utils.ResetSystemFunctions()

		err := platform.EnableUserLingering([]string{"host1", "host2"}, "path/to/gpHome", "serviceUser")
		var expectedErr *exec.ExitError
		if !errors.As(err, &expectedErr) {
			t.Errorf("got %T, want %T", err, expectedErr)
		}

		expectedErrPrefix := "could not enable user lingering:"
		if !strings.Contains(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
}

func ServiceStatusOutput() {
	os.Stdout.WriteString("got status of the service")
	os.Exit(0)
}

func ServiceStopped() {
	os.Exit(3)
}

func GetPlatform(t *testing.T, os string) platform.Platform {
	t.Helper()

	platform, err := platform.NewPlatform(os)
	if err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	return platform
}

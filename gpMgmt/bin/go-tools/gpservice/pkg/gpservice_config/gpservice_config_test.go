package gpservice_config_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gpservice/constants"
	"github.com/greenplum-db/gpdb/gpservice/internal/testutils/exectest"
	"github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
)

// Enable exectest.NewCommand mocking.
func TestMain(m *testing.M) {
	os.Exit(exectest.Run(m))
}

func TestConfig(t *testing.T) {
	testhelper.SetupTestLogger()
	
	expected := &gpservice_config.Config{
		HubPort: 1111,
		AgentPort: 2222,
		Hostnames: []string{"sdw1", "sdw2"},
		LogDir: "logdir",
		ServiceName: "serviceName",
		GpHome: "gphome",
		Credentials: &utils.GpCredentials{
			CACertPath: "ca-cert",
			ServerCertPath: "server-cert",
			ServerKeyPath: "server-key",
		},
	}

	t.Run("successfully stores the config on disk and able to read it back", func(t *testing.T) {
		var gpsyncCalled bool
		utils.System.ExecCommand = exectest.NewCommandWithVerifier(exectest.Success, func(utility string, args ...string) {
			gpsyncCalled = true

			expectedArgs := []string{"gpsync", "-h", expected.Hostnames[0], "-h", expected.Hostnames[1]}
			if !strings.Contains(strings.Join(args, " "), strings.Join(expectedArgs, " ")) {
				t.Fatalf("got %+v, want %+v", args, expectedArgs)
			}
		})
		defer utils.ResetSystemFunctions()
		
		filepath := filepath.Join(t.TempDir(), constants.ConfigFileName)
		err := gpservice_config.Create(filepath, expected.HubPort, expected.AgentPort, expected.Hostnames, expected.LogDir, expected.ServiceName, expected.GpHome, expected.Credentials)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		
		if !gpsyncCalled {
			t.Fatalf("expected gpsync to be called")
		}
		
		result, err := gpservice_config.Read(filepath)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		
		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("got %+v, want %+v", result, expected)
		}
	})
	
	t.Run("returns error when fails to create config file", func(t *testing.T) {
		expectedErr := os.ErrNotExist
		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return nil, expectedErr
		}
		defer utils.ResetSystemFunctions()
		
		expectedFilepath := "test.config"
		err := expected.Write(expectedFilepath)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}
		
		expectedErrPrefix := fmt.Sprintf("could not create service config file %s:", expectedFilepath)
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
	
	t.Run("returns error when fails to write to config file", func(t *testing.T) {
		expectedErr := os.ErrClosed
		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			_, writer, _ := os.Pipe()
			writer.Close()

			return writer, nil
		}
		defer utils.ResetSystemFunctions()
		
		expectedFilepath := "test.config"
		err := expected.Write(expectedFilepath)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}
		
		expectedErrPrefix := fmt.Sprintf("could not write to service config file %s:", expectedFilepath)
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
	
	t.Run("returns error when fails to copy config file to agents", func(t *testing.T) {
		utils.System.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
			_, writer, _ := os.Pipe()

			return writer, nil
		}		
		utils.System.ExecCommand = exectest.NewCommand(exectest.Failure)
		defer utils.ResetSystemFunctions()
		
		var expectedErr *exec.ExitError
		expectedFilepath := "test.config"
		err := expected.Write(expectedFilepath)
		if !errors.As(err, &expectedErr) {
			t.Errorf("got %T, want %T", err, expectedErr)
		}
		
		expectedErrPrefix := fmt.Sprintf("could not copy %s to segment hosts:", expectedFilepath)
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
	
	t.Run("returns error when fails to read the config file", func(t *testing.T) {
		expectedErr := os.ErrNotExist
		utils.System.ReadFile = func(name string) ([]byte, error) {
			return nil, expectedErr
		}
		defer utils.ResetSystemFunctions()
		
		expectedFilepath := "test.config"
		_, err := gpservice_config.Read(expectedFilepath)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}
		
		expectedErrPrefix := fmt.Sprintf("could not open service config file %s:", expectedFilepath)
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
	
	t.Run("returns error when fails to parse the config file", func(t *testing.T) {
		utils.System.ReadFile = func(name string) ([]byte, error) {
			return []byte(""), nil
		}
		defer utils.ResetSystemFunctions()
		
		var expectedErr *json.SyntaxError
		expectedFilepath := "test.config"
		_, err := gpservice_config.Read(expectedFilepath)
		if !errors.As(err, &expectedErr) {
			t.Fatalf("got %T, want %T", err, expectedErr)
		}

		expectedErrPrefix := fmt.Sprintf("could not parse service config file %s:", expectedFilepath)
		if !strings.HasPrefix(err.Error(), expectedErrPrefix) {
			t.Fatalf("got %v, want %s", err, expectedErrPrefix)
		}
	})
}
package configure

import (
	"fmt"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
	"github.com/greenplum-db/gpdb/gp/utils"
	"os"
	"testing"
)

const (
	defaultLogFile = "/tmp/gp_configure.log"
)

var (
	defaultServiceDir string
	serviceExt        string
	defaultGPConf     hub.Config
)

var (
	successOutput = []string{
		"[INFO] Created service file directory",
		"[INFO] Wrote hub service file",
		"[INFO] Wrote agent service file",
	}
	helpTxt = []string{
		"Configure gp as a systemd daemon",
		"Usage:",
		"Flags:",
		"Global Flags:",
	}
)

func init() {
	certPath := "/tmp/certificates"
	p := utils.GetPlatform()
	defaultServiceDir, serviceExt, _ = testutils.GetServiceDetails(p)
	cred := &utils.GpCredentials{
		CACertPath:     fmt.Sprintf("%s/%s", certPath, "ca-cert.pem"),
		CAKeyPath:      fmt.Sprintf("%s/%s", certPath, "ca-key.pem"),
		ServerCertPath: fmt.Sprintf("%s/%s", certPath, "server-cert.pem"),
		ServerKeyPath:  fmt.Sprintf("%s/%s", certPath, "server-key.pem"),
	}
	defaultGPConf = hub.Config{
		Port:        constants.DefaultHubPort,
		AgentPort:   constants.DefaultAgentPort,
		Hostnames:   []string{},
		LogDir:      constants.DefaultHubLogDir,
		ServiceName: constants.DefaultServiceName,
		GpHome:      testutils.GpHome,
		Credentials: cred,
	}
}

// TestMain function to run tests and perform cleanup at the end.
func TestMain(m *testing.M) {
	exitVal := m.Run()
	tearDownTest()

	os.Exit(exitVal)
}

func tearDownTest() {
	testutils.CleanupFiles(testutils.Hostfile,
		fmt.Sprintf("%s/%s", testutils.GpHome, constants.ConfigFileName))
}

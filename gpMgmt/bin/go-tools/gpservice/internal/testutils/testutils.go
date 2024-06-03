package testutils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/onsi/gomega/gbytes"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/idl/mock_idl"
	"github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
	config "github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
)

type MockPlatform struct {
	RetStatus            *idl.ServiceStatus
	ServiceStatusMessage string
	Err                  error
	ServiceFileContent   string
	DefServiceDir        string
	StartCmd             *exec.Cmd
	ConfigFileData       []byte
	OS                   string
}

func CreateDummyServiceConfig(t *testing.T) *config.Config {
	t.Helper()

	return &config.Config{
		HubPort:     1234,
		AgentPort:   5678,
		Hostnames:   []string{"sdw1", "sdw2"},
		LogDir:      "/tmp/logDir",
		ServiceName: "gp",
		GpHome:      "gpHome",
		Credentials: &MockCredentials{TlsConnection: insecure.NewCredentials()},
	}
}

func (p *MockPlatform) CreateServiceDir(hostnames []string, serviceDir string, gpHome string) error {
	return nil
}
func (p *MockPlatform) GetServiceStatusMessage(serviceName string) (string, error) {
	return p.ServiceStatusMessage, p.Err
}
func (p *MockPlatform) GenerateServiceFileContents(process string, gpHome string, serviceName string) string {
	return p.ServiceFileContent
}
func (p *MockPlatform) GetDefaultServiceDir() string {
	return p.DefServiceDir
}
func (p *MockPlatform) ReloadHubService(servicePath string) error {
	return p.Err
}
func (p *MockPlatform) ReloadAgentService(gpHome string, hostList []string, servicePath string) error {
	return p.Err
}
func (p *MockPlatform) CreateAndInstallHubServiceFile(gpHome string, serviceDir string, serviceName string) error {
	return p.Err
}
func (p *MockPlatform) CreateAndInstallAgentServiceFile(hostnames []string, gpHome string, serviceDir string, serviceName string) error {
	return p.Err
}
func (p *MockPlatform) GetStartHubCommand(serviceName string) *exec.Cmd {
	return p.StartCmd
}
func (p *MockPlatform) GetStartAgentCommandString(serviceName string) []string {
	return nil
}
func (p *MockPlatform) ParseServiceStatusMessage(message string) idl.ServiceStatus {
	return idl.ServiceStatus{Status: p.RetStatus.Status, Pid: p.RetStatus.Pid, Uptime: p.RetStatus.Uptime}
}
func (p *MockPlatform) DisplayServiceStatus(outfile io.Writer, serviceName string, statuses []*idl.ServiceStatus, skipHeader bool) {
}
func (p *MockPlatform) EnableUserLingering(hostnames []string, gpHome string, serviceUser string) error {
	return nil
}
func (p *MockPlatform) ReadFile(configFilePath string) (config *gpservice_config.Config, err error) {
	return nil, err
}
func (p *MockPlatform) SetServiceFileContent(content string) {
	p.ServiceFileContent = content
}

type MockCredentials struct {
	TlsConnection credentials.TransportCredentials
	Err           error
}

func (s *MockCredentials) LoadServerCredentials() (credentials.TransportCredentials, error) {
	return s.TlsConnection, s.Err
}

func (s *MockCredentials) LoadClientCredentials() (credentials.TransportCredentials, error) {
	return s.TlsConnection, s.Err
}

func (s *MockCredentials) SetCredsError(errMsg string) {
	s.Err = errors.New(errMsg)
}
func (s *MockCredentials) ResetCredsError() {
	s.Err = nil
}

func AssertLogMessage(t *testing.T, buffer *gbytes.Buffer, message string) {
	t.Helper()

	pattern, err := regexp.Compile(message)
	if err != nil {
		t.Fatalf("unexpected error when compiling regex: %#v", err)
	}

	if !pattern.MatchString(string(buffer.Contents())) {
		t.Fatalf("expected pattern '%s' not found in log '%s'", message, buffer.Contents())
	}
}

func AssertLogMessageCount(t *testing.T, buffer *gbytes.Buffer, message string, expectedCount int) {
	t.Helper()

	count := strings.Count(string(buffer.Contents()), message)
	if count != expectedCount {
		t.Fatalf("expected pattern %q found %d times in log %q, want %d", message, count, buffer.Contents(), expectedCount)
	}
}

func AssertLogMessageNotPresent(t *testing.T, buffer *gbytes.Buffer, message string) {
	t.Helper()

	pattern, err := regexp.Compile(message)
	if err != nil {
		t.Fatalf("unexpected error when compiling regex: %#v", err)
	}

	if pattern.MatchString(string(buffer.Contents())) {
		t.Fatalf("expected pattern '%s' found in log '%s'", message, buffer.Contents())
	}
}

func AssertFileContents(t *testing.T, filepath string, expected string) {
	t.Helper()

	result, err := os.ReadFile(filepath)
	if err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	if strings.TrimSpace(string(result)) != strings.TrimSpace(expected) {
		t.Fatalf("got %s, want %s", result, expected)
	}
}

func AssertFileContentsUnordered(t *testing.T, filepath string, expected string) {
	t.Helper()

	result, err := os.ReadFile(filepath)
	if err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(result)), "\n")
	expectedLines := strings.Split(strings.TrimSpace(expected), "\n")

	sort.Strings(lines)
	sort.Strings(expectedLines)

	if !reflect.DeepEqual(lines, expectedLines) {
		t.Fatalf("got %s, want %s", result, expected)
	}
}

func CreateMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mockdb := sqlx.NewDb(db, "sqlmock")
	return mockdb, mock
}

func CreateMockDBConn(t *testing.T, errs ...error) (*dbconn.DBConn, sqlmock.Sqlmock) {
	t.Helper()

	return getMockDBConn(t, false, errs...)
}

func CreateMockDBConnWithContext(t *testing.T, ctx context.Context, errs ...error) (*utils.DBConnWithContext, sqlmock.Sqlmock) {
	t.Helper()

	conn, mock := CreateMockDBConn(t, errs...)

	connWithContext := &utils.DBConnWithContext{
		DB:  conn,
		Ctx: ctx,
	}

	return connWithContext, mock
}

func CreateMockDBConnForUtilityMode(t *testing.T, errs ...error) (*dbconn.DBConn, sqlmock.Sqlmock) {
	t.Helper()

	return getMockDBConn(t, true, errs...)
}

func CreateAndConnectMockDB(t *testing.T, numConns int) (*dbconn.DBConn, sqlmock.Sqlmock) {
	t.Helper()

	connection, mock := CreateMockDBConn(t)

	testhelper.ExpectVersionQuery(mock, "7.0.0")
	connection.MustConnect(numConns)

	return connection, mock
}

func CreateAndConnectMockDBWithContext(t *testing.T, ctx context.Context, numConns int) (*utils.DBConnWithContext, sqlmock.Sqlmock) {
	t.Helper()

	conn, mock := CreateAndConnectMockDB(t, numConns)

	connWithContext := &utils.DBConnWithContext{
		DB:  conn,
		Ctx: ctx,
	}

	return connWithContext, mock
}

func getMockDBConn(t *testing.T, utility bool, errs ...error) (*dbconn.DBConn, sqlmock.Sqlmock) {
	t.Helper()

	mockdb, mock := CreateMockDB(t)

	driver := &testhelper.TestDriver{DB: mockdb, DBName: "testdb", User: "testrole"}
	if len(errs) > 0 {
		driver.ErrsToReturn = errs
	} else {
		if utility {
			driver.ErrsToReturn = []error{fmt.Errorf(`pq: unrecognized configuration parameter "gp_session_role"`)}
		}
	}

	connection := dbconn.NewDBConnFromEnvironment("testdb")
	connection.Driver = driver
	connection.Host = "testhost"
	connection.Port = 5432

	return connection, mock
}

// createDirectoryWithRemoveFail creates a directory where the removal operation will fail
func CreateDirectoryWithRemoveFail(dirPath string) error {
	// Create the directory
	if err := os.MkdirAll(dirPath, 0777); err != nil {
		err = os.Chmod(dirPath, 0777)
		return err

	}

	// Change permissions of the directory to read-only
	if err := os.Chmod(dirPath, 0400); err != nil {
		return err
	}

	return nil
}

func CaptureStdout(t *testing.T) (chan string, *os.File, func()) {
	t.Helper()

	stdout := make(chan string)

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	os.Stdout = writer
	resetStdout := func() {
		os.Stdout = oldStdout
	}

	go func() {
		out, _ := io.ReadAll(reader)
		stdout <- string(out)
	}()

	return stdout, writer, resetStdout
}

func MockStdin(t *testing.T, input string) func() {
	t.Helper()

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = writer.WriteString(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	writer.Close()

	oldStdin := os.Stdin
	os.Stdin = reader
	resetStdin := func() {
		os.Stdin = oldStdin
	}

	return resetStdin
}

func MockStdinWithWriter(t *testing.T) (*os.File, func()) {
	t.Helper()

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	oldStdin := os.Stdin
	os.Stdin = reader
	resetStdin := func() {
		os.Stdin = oldStdin
	}

	return writer, resetStdin
}

func CheckGRPCServerRunning(t *testing.T, port int) {
	t.Helper()

	conn, err := grpc.NewClient(net.JoinHostPort("localhost", strconv.Itoa(port)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer conn.Close()

	if conn.GetState() != connectivity.Idle {
		t.Fatalf("expected server to be in idle state")
	}
}

func GetPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("failed to listen on tcp:0")
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	return port
}

func GetAndListenOnPort(t *testing.T) (int, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("failed to listen on tcp:0")
	}

	port := listener.Addr().(*net.TCPAddr).Port
	cleanup := func() {
		listener.Close()
	}

	return port, cleanup
}

func ExecuteCobraCommand(t *testing.T, cmd *cobra.Command, args ...string) (string, error) {
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs(args)
	err := cmd.Execute()

	return buf.String(), err
}

func ExecuteCobraCommandContext(t *testing.T, ctx context.Context, cmd *cobra.Command, args ...string) (string, error) {
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(ctx)

	return buf.String(), err
}

func MockConnectToHub(t *testing.T, hubClient *mock_idl.MockHubClient) func() {
	t.Helper()

	oldConnectToHub := gpservice_config.ConnectToHub
	gpservice_config.ConnectToHub = func(conf *gpservice_config.Config) (idl.HubClient, error) {
		return hubClient, nil
	}

	return func() {
		gpservice_config.ConnectToHub = oldConnectToHub
	}
}

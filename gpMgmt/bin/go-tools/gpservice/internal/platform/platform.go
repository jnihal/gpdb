package platform

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gpservice/constants"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/pkg/greenplum"
	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
)

var (
	platform             Platform
)

type GpPlatform struct {
	OS         string
	ServiceCmd string // Binary for managing services
	UserArg    string // systemd always needs a "--user" flag passed, launchctl does not
	ServiceExt string // Extension for service files
	StatusArg  string // Argument passed to ServiceCmd to get status of a service
}

func NewPlatform(os string) (Platform, error) {
	switch os {
	case constants.PlatformDarwin:
		return GpPlatform{
			OS:         constants.PlatformDarwin,
			ServiceCmd: "launchctl",
			UserArg:    "",
			ServiceExt: "plist",
			StatusArg:  "list",
		}, nil

	case constants.PlatformLinux:
		return GpPlatform{
			OS:         constants.PlatformLinux,
			ServiceCmd: "systemctl",
			UserArg:    "--user",
			ServiceExt: "service",
			StatusArg:  "show",
		}, nil

	default:
		return nil, errors.New("unsupported OS")
	}
}

type Platform interface {
	CreateServiceDir(hostnames []string, serviceDir string, gpHome string) error
	GenerateServiceFileContents(process string, gpHome string, serviceName string) string
	GetDefaultServiceDir() string
	ReloadHubService(servicePath string) error
	ReloadAgentService(gpHome string, hostList []string, servicePath string) error
	CreateAndInstallHubServiceFile(gpHome string, serviceDir string, serviceName string) error
	CreateAndInstallAgentServiceFile(hostnames []string, gpHome string, serviceDir string, serviceName string) error
	GetStartHubCommand(serviceName string) *exec.Cmd
	GetStartAgentCommandString(serviceName string) []string
	GetServiceStatusMessage(serviceName string) (string, error)
	ParseServiceStatusMessage(message string) idl.ServiceStatus
	EnableUserLingering(hostnames []string, gpHome string, serviceUser string) error
}

func GetPlatform() Platform {
	var err error

	if platform == nil {
		platform, err = NewPlatform(runtime.GOOS)
		if err != nil {
			fmt.Printf("error: %s\n", err)
			os.Exit(1)
		}
	}

	return platform
}

func (p GpPlatform) CreateServiceDir(hostnames []string, serviceDir string, gpHome string) error {
	gpsshCmd := &greenplum.GpSSH{
		Hostnames: hostnames,
		Command: fmt.Sprintf("mkdir -p %s", serviceDir),
	}
	out, err := utils.RunGpSourcedCommand(gpsshCmd, gpHome)
	if err != nil {
		return fmt.Errorf("could not create service directory %s on hosts: %s, %w", serviceDir, out, err)
	}

	gplog.Info("Created service file directory %s on all hosts", serviceDir)
	return nil
}

func WriteServiceFile(filename string, contents string) error {
	handle, err := utils.System.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("could not create service file %s: %w\n", filename, err)
	}
	defer handle.Close()

	_, err = handle.WriteString(contents)
	if err != nil {
		return fmt.Errorf("could not write to service file %s: %w\n", filename, err)
	}

	return nil
}

func (p GpPlatform) GenerateServiceFileContents(process string, gpHome string, serviceName string) string {
	if p.OS == constants.PlatformDarwin {
		return GenerateDarwinServiceFileContents(process, gpHome, serviceName)
	}

	return GenerateLinuxServiceFileContents(process, gpHome, serviceName)
}

func GenerateDarwinServiceFileContents(process string, gpHome string, serviceName string) string {
	template := `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>%[3]s_%[1]s</string>
    <key>ProgramArguments</key>
    <array>
        <string>%[2]s/bin/gpservice</string>
        <string>%[1]s</string>
    </array>
    <key>StandardOutPath</key>
    <string>/tmp/grpc_%[1]s.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/grpc_%[1]s.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>%[4]s</string>
        <key>GPHOME</key>
        <string>%[2]s</string>
    </dict>
</dict>
</plist>
`
	return fmt.Sprintf(template, process, gpHome, serviceName, os.Getenv("PATH"))
}

func GenerateLinuxServiceFileContents(process string, gpHome string, serviceName string) string {
	template := `[Unit]
Description=Greenplum Database management utility %[1]s

[Service]
Type=simple
Environment=GPHOME=%[2]s
ExecStart=%[2]s/bin/gpservice %[1]s
Restart=on-failure
StandardOutput=file:/tmp/grpc_%[1]s.log
StandardError=file:/tmp/grpc_%[1]s.log

[Install]
Alias=%[3]s_%[1]s.service
WantedBy=default.target
`
	return fmt.Sprintf(template, process, gpHome, serviceName)
}

func (p GpPlatform) GetDefaultServiceDir() string {
	user, _ := utils.System.CurrentUser()

	if p.OS == constants.PlatformDarwin {
		return filepath.Join("/Users", user.Username, "Library", "LaunchAgents")
	}

	return filepath.Join("/home", user.Username, ".config", "systemd", "user")
}

func (p GpPlatform) CreateAndInstallHubServiceFile(gpHome string, serviceDir string, serviceName string) error {
	hubServiceContents := p.GenerateServiceFileContents("hub", gpHome, serviceName)
	hubServiceFilePath := filepath.Join(serviceDir, fmt.Sprintf("%s_hub.%s", serviceName, p.ServiceExt))
	err := WriteServiceFile(hubServiceFilePath, hubServiceContents)
	if err != nil {
		return err
	}

	err = p.ReloadHubService(hubServiceFilePath)
	if err != nil {
		return err
	}

	gplog.Info("Wrote hub service file to %s on coordinator host", hubServiceFilePath)
	return nil
}

func (p GpPlatform) ReloadHubService(servicePath string) error {
	if p.OS == constants.PlatformDarwin {
		// launchctl does not have a single reload command. Hence unload and load the file to update the configuration.
		out, err := utils.System.ExecCommand(p.ServiceCmd, "unload", servicePath).CombinedOutput()
		if err != nil {
			return fmt.Errorf("could not unload hub service file %s: %s, %w", servicePath, out, err)
		}

		out, err = utils.System.ExecCommand(p.ServiceCmd, "load", servicePath).CombinedOutput()
		if err != nil {
			return fmt.Errorf("could not load hub service file %s: %s, %w", servicePath, out, err)
		}

		return nil
	}

	out, err := utils.System.ExecCommand(p.ServiceCmd, p.UserArg, "daemon-reload").CombinedOutput()
	if err != nil {
		return fmt.Errorf("could not reload hub service file %s: %s, %w", servicePath, out, err)
	}

	return nil
}

func (p GpPlatform) ReloadAgentService(gpHome string, hostnames []string, servicePath string) error {
	if p.OS == constants.PlatformDarwin {
		// launchctl does not have a single reload command. Hence unload and load the file to update the configuration.
		gpsshCmd := &greenplum.GpSSH{
			Hostnames: hostnames,
			Command: fmt.Sprintf("%s unload %s", p.ServiceCmd, servicePath),
		}
		out, err := utils.RunGpSourcedCommand(gpsshCmd, gpHome)
		if err != nil {
			return fmt.Errorf("could not unload agent service file %s on segment hosts: %s, %w", servicePath, out, err)
		}

		gpsshCmd = &greenplum.GpSSH{
			Hostnames: hostnames,
			Command: fmt.Sprintf("%s load %s", p.ServiceCmd, servicePath),
		}
		out, err = utils.RunGpSourcedCommand(gpsshCmd, gpHome)
		if err != nil {
			return fmt.Errorf("could not load agent service file %s on segment hosts: %s, %w", servicePath, out, err)
		}

		return nil
	}

	gpsshCmd := &greenplum.GpSSH{
		Hostnames: hostnames,
		Command: fmt.Sprintf("%s daemon-reload", p.UserArg),
	}
	out, err := utils.RunGpSourcedCommand(gpsshCmd, gpHome)
	if err != nil {
		return fmt.Errorf("could not reload agent service file %s on segment hosts: %s, %w", servicePath, out, err)
	}

	return nil
}

func (p GpPlatform) CreateAndInstallAgentServiceFile(hostnames []string, gpHome string, serviceDir string, serviceName string) error {
	agentServiceContents := p.GenerateServiceFileContents("agent", gpHome, serviceName)
	localAgentServiceFilePath := fmt.Sprintf("./%s_agent.%s", serviceName, p.ServiceExt)
	err := WriteServiceFile(localAgentServiceFilePath, agentServiceContents)
	if err != nil {
		return err
	}
	defer os.Remove(localAgentServiceFilePath)

	remoteAgentServiceFilePath := fmt.Sprintf("%s/%s_agent.%s", serviceDir, serviceName, p.ServiceExt)
	gsyncCmd := &greenplum.GpSync{
		Hostnames: hostnames,
		Source: localAgentServiceFilePath,
		Destination: remoteAgentServiceFilePath,
	}
	out, err := utils.RunGpSourcedCommand(gsyncCmd, gpHome)
	if err != nil {
		return fmt.Errorf("could not copy agent service file to segment hosts: %s, %w", out, err)
	}

	err = p.ReloadAgentService(gpHome, hostnames, remoteAgentServiceFilePath)
	if err != nil {
		return err
	}

	gplog.Info("Wrote agent service file to %s on segment hosts", remoteAgentServiceFilePath)
	return nil
}

func (p GpPlatform) GetStartHubCommand(serviceName string) *exec.Cmd {
	args := []string{p.UserArg, "start", fmt.Sprintf("%s_hub", serviceName)}

	if p.OS == constants.PlatformDarwin { // empty strings are also treated as arguments
		args = args[1:]
	}

	return utils.System.ExecCommand(p.ServiceCmd, args...)
}

func (p GpPlatform) GetStartAgentCommandString(serviceName string) []string {
	return []string{p.ServiceCmd, p.UserArg, "start", fmt.Sprintf("%s_agent", serviceName)}
}

func (p GpPlatform) GetServiceStatusMessage(serviceName string) (string, error) {
	args := []string{p.UserArg, p.StatusArg, serviceName}

	if p.OS == constants.PlatformDarwin { // empty strings are also treated as arguments
		args = args[1:]
	}

	output, err := utils.System.ExecCommand(p.ServiceCmd, args...).CombinedOutput()
	if err != nil {
		if err.Error() != "exit status 3" { // 3 = service is stopped
			return "", fmt.Errorf("failed to get service status: %s, %w", output, err)
		}
	}

	return string(output), nil
}

/*
Example service status output

Linux:
ExecMainStartTimestamp=Sun 2023-08-20 14:43:35 UTC
ExecMainPID=83008
ExecMainCode=0
ExecMainStatus=0

Darwin:

	{
		"PID" = 19909;
		"Program" = "/usr/local/gpdb/bin/gpservice";
		"ProgramArguments" = (
			"/usr/local/gpdb/bin/gpservice";
			"hub";
		);
	};
*/
func (p GpPlatform) ParseServiceStatusMessage(message string) idl.ServiceStatus {
	var uptime string
	var pid int

	lines := strings.Split(message, "\n")
	for _, line := range lines {
		line = strings.TrimSuffix(strings.TrimSpace(line), ";")
		switch {
		case strings.HasPrefix(line, "\"PID\" ="): // for darwin
			results := strings.Split(line, " = ")
			pid, _ = strconv.Atoi(results[1])

		case strings.HasPrefix(line, "MainPID="): // for linux
			results := strings.Split(line, "=")
			pid, _ = strconv.Atoi(results[1])

		case strings.HasPrefix(line, "ActiveEnterTimestamp="): // for linux
			result := strings.Split(line, "=")
			uptime = result[1]
		}
	}

	status := "not running"
	if pid > 0 {
		status = "running"
	}

	return idl.ServiceStatus{Status: status, Uptime: uptime, Pid: uint32(pid)}
}

// Allow systemd services to run on startup and be started/stopped without root access
// This is a no-op on Mac, as launchctl lacks the concept of user lingering
func (p GpPlatform) EnableUserLingering(hostnames []string, gpHome string, serviceUser string) error {
	if p.OS != "linux" {
		return nil
	}
	
	gpsshCmd := &greenplum.GpSSH{
		Hostnames: hostnames,
		Command: fmt.Sprintf("loginctl enable-linger %s", serviceUser),
	}
	out, err := utils.RunGpSourcedCommand(gpsshCmd, gpHome)
	if err != nil {
		return fmt.Errorf("could not enable user lingering: %s, %w", out, err)
	}

	return nil
}

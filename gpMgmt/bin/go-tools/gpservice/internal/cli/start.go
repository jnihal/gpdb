package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
)

func StartCmd() *cobra.Command {
	var startHub, startAgent bool

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start hub and agent services",
		Example: `Start the hub and agent services
$ gpservice start

To start only the hub service
$ gpservice start --hub

To start only the agent service
$ gpservice start --agent
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if startHub {
				return startHubService(conf)
			} else if startAgent {
				return startAgentService(conf)
			} else {
				return StartServices(conf)
			}
		},
	}

	startCmd.Flags().BoolVar(&startHub, "hub", false, "Start only the hub service")
	startCmd.Flags().BoolVar(&startAgent, "agent", false, "Start only the agent service. Hub service should already be running")

	return startCmd
}

func startHubService(conf *gpservice_config.Config) error {
	out, err := platform.GetStartHubCommand(conf.ServiceName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start hub service: %s, %w", out, err)
	}

	gplog.Info("Hub service started successfully")
	if verbose {
		if verbose {
			status, _ := getHubStatus(conf)
			displayServiceStatus(os.Stdout, status)
		}
	}

	return nil
}

func startAgentService(conf *gpservice_config.Config) error {
	client, err := gpservice_config.ConnectToHub(conf)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.StartAgents(ctx, &idl.StartAgentsRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("failed to start agent service: %w", err)
	}

	gplog.Info("Agent service started successfully")
	if verbose {
		status, _ := getAgentStatus(conf)
		displayServiceStatus(os.Stdout, status)
	}

	return nil
}

func StartServices(conf *gpservice_config.Config) error {
	err := startHubService(conf)
	if err != nil {
		return err
	}

	err = startAgentService(conf)
	if err != nil {
		return err
	}

	return nil
}

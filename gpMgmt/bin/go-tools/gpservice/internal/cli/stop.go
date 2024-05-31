package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func StopCmd() *cobra.Command {
	var stopHub, stopAgent bool

	stopCmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop hub and agent services",
		RunE: func(cmd *cobra.Command, args []string) error {
			if stopHub {
				return stopHubService(conf)
			} else if stopAgent {
				return stopAgentService(conf)
			} else {
				return StopServices(conf)
			}
		},
	}

	stopCmd.Flags().BoolVar(&stopHub, "hub", false, "Stop only the hub service")
	stopCmd.Flags().BoolVar(&stopAgent, "agent", false, "Stop only the agent service. Hub service should already be running")

	return stopCmd
}

func stopHubService(conf *gpservice_config.Config) error {
	client, err := gpservice_config.ConnectToHub(conf)
	if err != nil {
		return err
	}

	_, err = client.Stop(context.Background(), &idl.StopHubRequest{})
	// Ignore a "hub already stopped" error
	if err != nil {
		errCode := grpcStatus.Code(err)
		errMsg := grpcStatus.Convert(err).Message()
		// XXX: "transport is closing" is not documented but is needed to uniquely interpret codes.Unavailable
		// https://github.com/grpc/grpc/blob/v1.24.0/doc/statuscodes.md
		if errCode != codes.Unavailable || errMsg != "transport is closing" {
			return fmt.Errorf("failed to stop hub service: %w", err)
		}
	}

	gplog.Info("Hub service stopped successfully")
	return nil
}

func stopAgentService(conf *gpservice_config.Config) error {
	client, err := gpservice_config.ConnectToHub(conf)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.StopAgents(ctx, &idl.StopAgentsRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("failed to stop agent service: %w", err)
	}

	gplog.Info("Agent service stopped successfully")
	return nil
}

func StopServices(conf *gpservice_config.Config) error {
	err := stopAgentService(conf)
	if err != nil {
		return err
	}

	err = stopHubService(conf)
	if err != nil {
		return err
	}

	return nil
}

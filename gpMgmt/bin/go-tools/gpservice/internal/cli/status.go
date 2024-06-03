package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/greenplum-db/gpdb/gpservice/idl"
	"github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
)

func StatusCmd() *cobra.Command {
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Display status of hub and agent services",
		RunE: func(cmd *cobra.Command, args []string) error {
			var statuses []*idl.ServiceStatus
			
			hubStatus, err := getHubStatus(conf)
			if err != nil {
				return err
			}
			statuses = append(statuses, hubStatus...)
			
			agentStatus, err := getAgentStatus(conf)
			if err != nil {
				displayServiceStatus(os.Stdout, statuses)
				return err
			}
			statuses = append(statuses, agentStatus...)
			
			displayServiceStatus(os.Stdout, statuses)
			
			return nil
		},
	}

	return statusCmd
}

func getHubStatus(conf *gpservice_config.Config) ([]*idl.ServiceStatus, error) {
	message, err := platform.GetServiceStatusMessage(fmt.Sprintf("%s_hub", conf.ServiceName))
	if err != nil {
		return nil, err
	}

	status := platform.ParseServiceStatusMessage(message)
	status.Host, _ = utils.System.GetHostName()
	status.Role = "Hub"

	return []*idl.ServiceStatus{&status}, nil
}

func getAgentStatus(conf *gpservice_config.Config) ([]*idl.ServiceStatus, error) {
	client, err := gpservice_config.ConnectToHub(conf)
	if err != nil {
		return nil, err
	}

	reply, err := client.StatusAgents(context.Background(), &idl.StatusAgentsRequest{})
	if err != nil {
		return nil, err
	}

	return reply.Statuses, nil
}

func displayServiceStatus(outfile io.Writer, statuses []*idl.ServiceStatus) {
	w := new(tabwriter.Writer)
	w.Init(outfile, 0, 8, 2, '\t', 0)
	fmt.Fprintln(w, "ROLE\tHOST\tSTATUS\tPID\tUPTIME")

	for _, s := range statuses {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n", s.Role, s.Host, s.Status, s.Pid, s.Uptime)
	}
	w.Flush()
}

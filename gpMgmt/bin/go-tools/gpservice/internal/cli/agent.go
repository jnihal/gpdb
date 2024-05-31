package cli

import (
	"github.com/greenplum-db/gpdb/gpservice/internal/agent"
	"github.com/spf13/cobra"
)

func AgentCmd() *cobra.Command {
	agentCmd := &cobra.Command{
		Use:    "agent",
		Short:  "Start gpservice as an agent process",
		Long:   "Start gpservice as an agent process",
		Hidden: true, // Should not be invoked by the user
		RunE: func(cmd *cobra.Command, args []string) error {
			agentConf := agent.Config{
				Port:        conf.AgentPort,
				ServiceName: conf.ServiceName,
				GpHome:      conf.GpHome,
				Credentials: conf.Credentials,
				LogDir:      conf.LogDir,
			}
			a := agent.New(agentConf)

			err := a.Start()
			if err != nil {
				return err
			}

			return nil
		},
	}

	return agentCmd
}

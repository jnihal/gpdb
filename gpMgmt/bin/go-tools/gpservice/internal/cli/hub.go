package cli

import (
	"github.com/spf13/cobra"

	"github.com/greenplum-db/gpdb/gpservice/internal/hub"
)

func HubCmd() *cobra.Command {
	hubCmd := &cobra.Command{
		Use:    "hub",
		Short:  "Start gpservice as an agent process",
		Long:   "Start gpservice as an agent process",
		Hidden: true, // Should not be invoked by the user
		RunE: func(cmd *cobra.Command, args []string) error {
			h := hub.New(conf, nil)
			err := h.Start()
			if err != nil {
				return err
			}

			return nil
		},
	}

	return hubCmd
}

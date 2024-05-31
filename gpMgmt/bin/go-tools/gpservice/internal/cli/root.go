package cli

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gpservice/constants"
	config "github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
	"github.com/spf13/cobra"
)

var (
	configFilepath string
	conf           *config.Config
	verbose        bool
)

func RootCommand() *cobra.Command {
	root := &cobra.Command{
		Use: "gpservice",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			// gpservice configuration is created after the init command
			if cmd.Name() == "init" {
				initializeLogger(cmd, hubLogDir)
				return
			}

			conf, err = config.Read(configFilepath)
			if err != nil {
				return err
			}

			initializeLogger(cmd, conf.LogDir)
			return
		}}

	root.PersistentFlags().StringVar(&configFilepath, "config-file", filepath.Join(os.Getenv("GPHOME"), constants.ConfigFileName), `Path to gpservice configuration file`)
	root.PersistentFlags().BoolVar(&verbose, "verbose", false, `Provide verbose output`)

	root.CompletionOptions.DisableDefaultCmd = true

	root.AddCommand(
		AgentCmd(),
		configureCmd(),
		HubCmd(),
		StartCmd(),
		StatusCmd(),
		StopCmd(),
	)

	return root
}

func initializeLogger(cmd *cobra.Command, logdir string) {
	// CommandPath lists the names of the called command and all of its parent commands, so this
	// turns e.g. "gp stop hub" into "gp_stop_hub" to generate a unique log file name for each command.
	logName := strings.ReplaceAll(cmd.CommandPath(), " ", "_")
	gplog.InitializeLogging(logName, logdir)

	if verbose {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

// used only for testing
func SetConf(customConf *config.Config) func() {
	oldConf := conf
	conf = customConf

	return func() {
		conf = oldConf
	}
}

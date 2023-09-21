package cli

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

var (
	ShowHubStatus       = ShowHubStatusFn
	ShowAgentsStatus    = ShowAgentsStatusFn
	PrintServicesStatus = PrintServicesStatusFn
)

func statusCmd() *cobra.Command {
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Display status",
	}

	statusCmd.AddCommand(statusHubCmd())
	statusCmd.AddCommand(statusAgentsCmd())
	statusCmd.AddCommand(statusServicesCmd())
	statusCmd.AddCommand(parallelAgentsCmd())

	return statusCmd
}

func statusHubCmd() *cobra.Command {
	statusHubCmd := &cobra.Command{
		Use:     "hub",
		Short:   "Display hub status",
		PreRunE: InitializeCommand,
		RunE:    RunStatusHub,
	}

	return statusHubCmd
}

func RunStatusHub(cmd *cobra.Command, args []string) error {
	_, err := ShowHubStatus(Conf, false)
	if err != nil {
		return fmt.Errorf("Could not retrieve hub status: %w", err)
	}

	return nil
}

func statusAgentsCmd() *cobra.Command {
	statusAgentsCmd := &cobra.Command{
		Use:     "agents",
		Short:   "Display agents status",
		PreRunE: InitializeCommand,
		RunE:    RunStatusAgent,
	}

	return statusAgentsCmd
}

func parallelAgentsCmd() *cobra.Command {
	statusAgentsCmd := &cobra.Command{
		Use:     "parallel",
		Short:   "Display agents status",
		PreRunE: InitializeCommand,
		RunE:    RunParallelAgent,
	}

	return statusAgentsCmd
}

func statusServicesCmd() *cobra.Command {
	statusServicesCmd := &cobra.Command{
		Use:     "services",
		Short:   "Display Hub and Agent services status",
		PreRunE: InitializeCommand,
		RunE:    RunServiceStatus,
	}

	return statusServicesCmd
}

func RunStatusAgent(cmd *cobra.Command, args []string) error {
	err := ShowAgentsStatus(Conf, false)
	if err != nil {
		return fmt.Errorf("Could not retrieve agents status: %w", err)
	}

	return nil
}

func RunParallelAgent(cmd *cobra.Command, args []string) error {
	err := RunParallelCmd(Conf)
	if err != nil {
		return fmt.Errorf("Could not run parallel: %w", err)
	}

	return nil
}

func ShowHubStatusFn(conf *hub.Config, skipHeader bool) (bool, error) {
	message, err := Platform.GetServiceStatusMessage(fmt.Sprintf("%s_hub", conf.ServiceName))
	if err != nil {
		return false, err
	}
	status := Platform.ParseServiceStatusMessage(message)
	status.Host, _ = os.Hostname()
	Platform.DisplayServiceStatus(os.Stdout, "Hub", []*idl.ServiceStatus{&status}, skipHeader)
	if status.Status == "Unknown" {
		return false, nil
	}

	return true, nil
}

func ShowAgentsStatusFn(conf *hub.Config, skipHeader bool) error {
	client, err := ConnectToHub(conf)
	if err != nil {
		return fmt.Errorf("Could not connect to hub; is the hub running? Error:%w", err)
	}

	reply, err := client.StatusAgents(context.Background(), &idl.StatusAgentsRequest{})
	if err != nil {
		return err
	}
	Platform.DisplayServiceStatus(os.Stdout, "Agent", reply.Statuses, skipHeader)

	return nil
}

func RunParallelCmd(conf *hub.Config) error {
	client, err := ConnectToHub(conf)
	if err != nil {
		return fmt.Errorf("Could not connect to hub; is the hub running? Error:%w", err)
	}

	req := idl.DummyHubRequest{}
	for i := 1; i < 11; i++ {
		req.Input = append(req.Input, &idl.DummyInput{Host: "sdw1", Value: uint32(i)})
		req.Input = append(req.Input, &idl.DummyInput{Host: "sdw2", Value: uint32(i)})
		req.Input = append(req.Input, &idl.DummyInput{Host: "sdw3", Value: uint32(i)})
	}

	reply, err := client.DummyHub(context.Background(), &req)
	if err != nil {
		return err
	}
	done := make(chan bool)

	p := mpb.New(mpb.WithWidth(64))

	total := len(req.Input)
	name := "Initializing segments:"
	// create a single bar, which will inherit container's width
	bar := p.AddBar(int64(total),
		// BarFillerBuilder with custom style
		mpb.PrependDecorators(
			// display our name with one space on the right
			decor.Name(name, decor.WC{W: len(name) + 1, C: decor.DidentRight}),
			
			decor.CountersNoUnit("%d/%d"),
			
			// replace ETA decorator with "done" message, OnComplete event
			decor.Elapsed(decor.ET_STYLE_GO, decor.WC{W: 4}),
		),
		mpb.AppendDecorators(
			decor.OnComplete(
				decor.Percentage(decor.WC{W: 4}), "done",
			),
		),
	)

	go func() {
		for {
			_, err := reply.Recv()
			if err == io.EOF {
				done <- true //means stream is finished
				return
			}
			if err != nil {
				log.Printf("cannot receive %v", err)
			}
			// log.Printf("Resp received: %s", resp.Out)
			bar.Increment()
		}
	}()

	<-done //we will wait until all response is received
	p.Wait()
	log.Printf("finished")
	return nil
}

func RunServiceStatus(cmd *cobra.Command, args []string) error {
	err := PrintServicesStatus()
	if err != nil {
		return fmt.Errorf("Error while getting the services status:%w", err)
	}

	return nil
}

func PrintServicesStatusFn() error {
	// TODO: Check if Hub is down, do not check for Agents
	hubRunning, err := ShowHubStatus(Conf, false)
	if err != nil {
		return fmt.Errorf("Error while showing the Hub status:%w", err)
	}
	if !hubRunning {
		fmt.Println("Hub service not running, not able to fetch agent status.")
		return nil
	}
	err = ShowAgentsStatus(Conf, true)
	if err != nil {
		return fmt.Errorf("Error while showing the Agent status:%w", err)
	}

	return nil
}

package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
)

var num_seg int

func initCmd() *cobra.Command {
	agentCmd := &cobra.Command{
		Use:     "init",
		Short:   "Start a gp process in agent mode",
		Long:    "Start a gp process in agent mode",
		Hidden:  true, // Should only be invoked by systemd
		PreRunE: InitializeCommand,
		RunE:    RunInit,
		
	}
	
	agentCmd.Flags().IntVar(&num_seg, "n", 5, ``)

	return agentCmd
}

func RunInit(cmd *cobra.Command, args []string) error {
	client, err := ConnectToHub(Conf)
	if err != nil {
		return fmt.Errorf("could not connect to hub; is the hub running? Error: %v", err)
	}
	
	configParams := make(map[string]string)
	configParams["key1"] = "value1"
	configParams["key2"] = "value2"
	
	segments := []*idl.Segment{}
	
	for i := 0; i < num_seg; i++ {
		seg := &idl.Segment{
			Contentid: -1,
			DataDirectory: fmt.Sprintf("/tmp/make%d", i),
			Dbid: 0,
			HostAddress: "non aliqua",
			HostName: "localhost",
			Port: 1111,
		}
		segments = append(segments, seg)
	}
	req := &idl.MakeClusterRequest{
		GpArray: &idl.GpArray{
			Primaries: segments,
		},
		ClusterParams: &idl.ClusterParams{
			CommonConfig: configParams,
		},
	}
	stream, err := client.MakeCluster(context.Background(), req)
	if err != nil {
		return err
	}

	instance := utils.NewProgressInstance()

	var label string
	var bar *mpb.Bar
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			if bar != nil {
				instance.Abort(bar, false)
				instance.Wait()
			}
			return err
		}

		switch resp := resp.Message.(type) {
		case *idl.MakeClusterReply_ProgressMessage:
			if label != resp.ProgressMessage.Label {
				label = resp.ProgressMessage.Label
				bar = utils.NewProgressBar(instance, label, len(req.GpArray.Primaries))
			} else {
				bar.Increment()
			}
		}
	}

	instance.Wait()
	return nil
}

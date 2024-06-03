package hub

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	"google.golang.org/grpc"
)

type RpcReply struct {
	hostname, address string
}

// ConnectHostList connects to given hostlist on the agent port, returns map of agent-address and connection
// if fails, returns error.
func (s *Server) ConnectHostList(hostList []string) (map[string]idl.AgentClient, error) {
	addressConnectionMap := make(map[string]idl.AgentClient)

	credentials, err := s.Credentials.LoadClientCredentials()
	if err != nil {
		return nil, err
	}

	for _, address := range hostList {
		if _, ok := addressConnectionMap[address]; !ok {
			address := net.JoinHostPort(address, strconv.Itoa(s.AgentPort))
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials))
			if err != nil {
				return nil, fmt.Errorf("could not connect to agent on host %s: %w", address, err)
			}

			addressConnectionMap[address] = idl.NewAgentClient(conn)
		}
	}

	return addressConnectionMap, nil
}
func (s *Server) GetAllHostNames(ctx context.Context, request *idl.GetAllHostNamesRequest) (*idl.GetAllHostNamesReply, error) {
	gplog.Debug("Starting with rpc GetAllHostNames")
	addressConnectionMap, err := s.ConnectHostList(request.HostList)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(addressConnectionMap))
	replies := make(chan RpcReply, len(addressConnectionMap))
	for address, conn := range addressConnectionMap {
		wg.Add(1)

		go func(addr string, connection idl.AgentClient) {
			defer wg.Done()
			request := idl.GetHostNameRequest{}
			reply, err := connection.GetHostName(context.Background(), &request)
			if err != nil {
				errs <- fmt.Errorf("host: %s, %w", addr, err)
				errs <- utils.LogAndReturnError(fmt.Errorf("getting hostname for %s failed with error:%v", addr, err))
				return
			}

			result := new(RpcReply)
			result.address = addr
			result.hostname = reply.Hostname
			replies <- *result
		}(address, conn)
	}
	wg.Wait()
	close(replies)
	close(errs)

	// Check for errors
	if len(errs) > 0 {
		for e := range errs {
			err = e
			break
		}
		return &idl.GetAllHostNamesReply{}, err
	}

	// Extract replies and populate reply
	hostNameMap := make(map[string]string)
	for reply := range replies {
		hostNameMap[reply.address] = reply.hostname
	}

	return &idl.GetAllHostNamesReply{HostNameMap: hostNameMap}, nil
}

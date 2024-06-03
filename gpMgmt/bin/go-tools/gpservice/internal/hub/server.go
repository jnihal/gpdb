package hub

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gpservice/idl"
	. "github.com/greenplum-db/gpdb/gpservice/internal/platform"
	. "github.com/greenplum-db/gpdb/gpservice/pkg/gpservice_config"
	"github.com/greenplum-db/gpdb/gpservice/pkg/greenplum"
	"github.com/greenplum-db/gpdb/gpservice/pkg/utils"
)

var (
	platform    = GetPlatform()
	DialTimeout = 3 * time.Second
)

type Server struct {
	*Config
	Conns      []*Connection
	mutex      sync.Mutex
	grpcServer *grpc.Server
	listener   net.Listener
	finish     chan struct{}
}

type Connection struct {
	Conn        *grpc.ClientConn
	AgentClient idl.AgentClient
	Hostname    string
}

func New(conf *Config) *Server {
	h := &Server{
		Config:     conf,
		finish:     make(chan struct{}, 1),
	}
	return h
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", s.HubPort)) // TODO: make this "hostname:port" so it can be started from somewhere other than the coordinator host
	if err != nil {
		return fmt.Errorf("could not listen on port %d: %w", s.HubPort, err)
	}

	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		// handle stuff here if needed
		return handler(ctx, req)
	}

	credentials, err := s.Credentials.LoadServerCredentials()
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials),
		grpc.UnaryInterceptor(interceptor),
	)

	s.mutex.Lock()
	s.grpcServer = grpcServer
	s.listener = listener
	s.mutex.Unlock()

	idl.RegisterHubServer(grpcServer, s)
	reflection.Register(grpcServer)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		<-s.finish
		gplog.Info("Received stop command, attempting graceful shutdown")
		s.grpcServer.GracefulStop()
		gplog.Info("gRPC server has shut down")
		wg.Done()
	}()

	err = grpcServer.Serve(listener)
	if err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
	wg.Wait()
	return nil
}

func (s *Server) Stop(ctx context.Context, in *idl.StopHubRequest) (*idl.StopHubReply, error) {
	s.Shutdown()
	return &idl.StopHubReply{}, nil
}

func (s *Server) Shutdown() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.grpcServer != nil {
		s.finish <- struct{}{}
	}
}

func (s *Server) StartAgents(ctx context.Context, in *idl.StartAgentsRequest) (*idl.StartAgentsReply, error) {
	err := s.StartAllAgents()
	if err != nil {
		return &idl.StartAgentsReply{}, err
	}

	// Make sure service has started :
	err = s.DialAllAgents()
	if err != nil {
		return &idl.StartAgentsReply{}, err
	}
	return &idl.StartAgentsReply{}, nil
}

func (s *Server) StartAllAgents() error {
	gpsshCmd := &greenplum.GpSSH{
		Hostnames: s.Hostnames,
		Command:   strings.Join(platform.GetStartAgentCommandString(s.ServiceName), " "),
	}
	out, err := utils.RunGpSourcedCommand(gpsshCmd, s.GpHome)
	if err != nil {
		return fmt.Errorf("could not start agents: %s, %w", out, err)
	}

	// As command is run through gpssh, even if actual command returns error, gpssh still returns as success.
	// to overcome this we have added check the command output.
	if strings.Contains(out.String(), "ERROR") || strings.Contains(out.String(), "No such file or directory") {
		return fmt.Errorf("could not start agents: %s, %w", out, err)
	}

	return nil
}

func (s *Server) DialAllAgents() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.Conns != nil {
		return nil
	}

	for _, host := range s.Hostnames {
		credentials, err := s.Credentials.LoadClientCredentials()
		if err != nil {
			return err
		}

		address := net.JoinHostPort(host, strconv.Itoa(s.AgentPort))
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials))
		if err != nil {
			return fmt.Errorf("could not connect to agent on host %s: %w", host, err)
		}

		s.Conns = append(s.Conns, &Connection{
			Conn:        conn,
			AgentClient: idl.NewAgentClient(conn),
			Hostname:    host,
		})
	}

	return nil
}

func (s *Server) StopAgents(ctx context.Context, in *idl.StopAgentsRequest) (*idl.StopAgentsReply, error) {
	request := func(conn *Connection) error {
		_, err := conn.AgentClient.Stop(context.Background(), &idl.StopAgentRequest{})
		if err == nil { // no error -> didn't stop
			return fmt.Errorf("failed to stop agent on host %s", conn.Hostname)
		}

		errStatus := grpcStatus.Convert(err)
		if errStatus.Code() != codes.Unavailable {
			return fmt.Errorf("failed to stop agent on host %s: %w", conn.Hostname, err)
		}

		return nil
	}

	err := s.DialAllAgents()
	if err != nil {
		return &idl.StopAgentsReply{}, err
	}

	err = ExecuteRPC(s.Conns, request)
	s.Conns = nil

	return &idl.StopAgentsReply{}, err
}

func (s *Server) StatusAgents(ctx context.Context, in *idl.StatusAgentsRequest) (*idl.StatusAgentsReply, error) {
	statusChan := make(chan *idl.ServiceStatus, len(s.Conns))

	request := func(conn *Connection) error {
		status, err := conn.AgentClient.Status(context.Background(), &idl.StatusAgentRequest{})
		if err != nil {
			return fmt.Errorf("failed to get agent status on host %s", conn.Hostname)
		}
		s := idl.ServiceStatus{
			Role:   "Agent",
			Host:   conn.Hostname,
			Status: status.Status,
			Uptime: status.Uptime,
			Pid:    status.Pid,
		}
		statusChan <- &s

		return nil
	}

	err := s.DialAllAgents()
	if err != nil {
		return &idl.StatusAgentsReply{}, err
	}
	err = ExecuteRPC(s.Conns, request)
	if err != nil {
		return &idl.StatusAgentsReply{}, err
	}
	close(statusChan)

	statuses := make([]*idl.ServiceStatus, 0)
	for status := range statusChan {
		statuses = append(statuses, status)
	}

	return &idl.StatusAgentsReply{Statuses: statuses}, err
}

func ExecuteRPC(agentConns []*Connection, executeRequest func(conn *Connection) error) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(agentConns))

	for _, conn := range agentConns {
		conn := conn
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := executeRequest(conn)
			if err != nil {
				errs <- fmt.Errorf("host: %s, %w", conn.Hostname, err)
			}
		}()
	}

	wg.Wait()
	close(errs)

	var err error
	for e := range errs {
		err = e
		break
	}

	return err
}

func getConnForHosts(conns []*Connection, hostnames []string) []*Connection {
	result := []*Connection{}
	for _, conn := range conns {
		if slices.Contains(hostnames, conn.Hostname) {
			result = append(result, conn)
		}
	}

	return result
}

package hub

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
	"github.com/greenplum-db/gpdb/gp/utils/greenplum"
)

// UpdatePgHbaConfWithMirrorEntries updates the pg_hba.conf file on the primary segments
// with the details of its corresponding mirror segment pair. The hbaHostname parameter
// determines whether to use hostnames or IP addresses in the pg_hba.conf file.
func (s *Server) UpdatePgHbaConfWithMirrorEntries(gparray *greenplum.GpArray, mirrorSegs []*idl.Segment, hbaHostname bool) error {
	primaryHostToSegPairMap := make(map[string][]*greenplum.SegmentPair)
	for _, seg := range mirrorSegs {
		pair, err := gparray.GetSegmentPairForContent(int(seg.Contentid))
		if err != nil {
			return err
		}

		primaryHostToSegPairMap[pair.Primary.Hostname] = append(primaryHostToSegPairMap[pair.Primary.Hostname], pair)
	}

	request := func(conn *Connection) error {
		var wg sync.WaitGroup

		pairs := primaryHostToSegPairMap[conn.Hostname]
		errs := make(chan error, len(pairs))
		for _, pair := range pairs {
			pair := pair
			wg.Add(1)

			go func(pair *greenplum.SegmentPair) {
				var addrs []string
				var err error
				defer wg.Done()

				if hbaHostname {
					addrs = []string{pair.Primary.Address, pair.Mirror.Address}
				} else {
					primaryAddrs, err := s.GetInterfaceAddrs(pair.Primary.Hostname)
					if err != nil {
						errs <- err
						return
					}

					mirrorAddrs, err := s.GetInterfaceAddrs(pair.Mirror.Hostname)
					if err != nil {
						errs <- err
						return
					}

					addrs = append(primaryAddrs, mirrorAddrs...)
				}

				_, err = conn.AgentClient.UpdatePgHbaConf(context.Background(), &idl.UpdatePgHbaConfRequest{
					Pgdata:      pair.Primary.DataDir,
					Addrs:       addrs,
					Replication: true,
				})
				if err != nil {
					errs <- err
				}
			}(pair)
		}

		wg.Wait()
		close(errs)

		var err error
		for e := range errs {
			err = errors.Join(err, e)
		}

		return err
	}

	return ExecuteRPC(s.Conns, request)
}

// GetInterfaceAddrs returns the interface addresses for a given host.
// It retrieves the interface addresses by executing an RPC call to the agent client.
func (s *Server) GetInterfaceAddrs(host string) ([]string, error) {
	conns := getConnForHosts(s.Conns, []string{host})

	var addrs []string
	request := func(conn *Connection) error {
		resp, err := conn.AgentClient.GetInterfaceAddrs(context.Background(), &idl.GetInterfaceAddrsRequest{})
		if err != nil {
			return fmt.Errorf("failed to get interface addresses for host %s: %w", conn.Hostname, err)
		}
		addrs = resp.Addrs

		return nil
	}

	err := ExecuteRPC(conns, request)

	return addrs, err
}

// ValidateDataChecksums validates the data page checksum version for all segments in the Greenplum cluster.
// It compares the data page checksum version of each segment with the coordinator's data page checksum version.
// If any segment has a different data page checksum version, an error is returned.
func (s *Server) ValidateDataChecksums(gparray *greenplum.GpArray) error {
	var coordinatorValue string
	request := func(conn *Connection) error {
		if conn.Hostname == gparray.Coordinator.Hostname {
			resp, err := conn.AgentClient.PgControlData(context.Background(), &idl.PgControlDataRequest{Pgdata: gparray.Coordinator.DataDir})
			if err != nil {
				return utils.FormatGrpcError(err)
			}

			coordinatorValue = resp.Result["Data page checksum version"]
			return nil
		}

		return nil
	}

	err := ExecuteRPC(s.Conns, request)
	if err != nil {
		return err
	}

	segmentDataChecksum := make(map[int]string)
	segmentDataChecksumMutex := sync.RWMutex{}
	hostToSegMap := gparray.GetSegmentsByHost()
	request = func(conn *Connection) error {
		var wg sync.WaitGroup

		segs := hostToSegMap[conn.Hostname]
		errs := make(chan error, len(segs))
		for _, seg := range segs {
			seg := seg
			wg.Add(1)

			go func(seg *greenplum.Segment) {
				defer wg.Done()

				resp, err := conn.AgentClient.PgControlData(context.Background(), &idl.PgControlDataRequest{Pgdata: seg.DataDir})
				if err != nil {
					errs <- utils.FormatGrpcError(err)
					return
				}

				segmentDataChecksumMutex.Lock()
				segmentDataChecksum[seg.Dbid] = resp.Result["Data page checksum version"]
				segmentDataChecksumMutex.Unlock()
			}(&seg)
		}

		wg.Wait()
		close(errs)

		var err error
		for e := range errs {
			err = errors.Join(err, e)
		}

		return err
	}

	err = ExecuteRPC(s.Conns, request)
	if err != nil {
		return err
	}

	var inconsistentSegs []int
	for key, value := range segmentDataChecksum {
		if value != coordinatorValue {
			inconsistentSegs = append(inconsistentSegs, key)
		}
	}

	if len(inconsistentSegs) != 0 {
		return fmt.Errorf("data page checksum version for segments with dbid %+v does not match the coordinator value of %s", inconsistentSegs, coordinatorValue)
	}

	return nil
}

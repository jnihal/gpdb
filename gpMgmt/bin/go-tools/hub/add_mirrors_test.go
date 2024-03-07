package hub_test

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/hub"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/idl/mock_idl"
	"github.com/greenplum-db/gpdb/gp/testutils"
	"github.com/greenplum-db/gpdb/gp/utils/greenplum"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	gparray     *greenplum.GpArray
	coordinator *greenplum.Segment
	primary1    *greenplum.Segment
	primary2    *greenplum.Segment
	mirror1     *greenplum.Segment
	mirror2     *greenplum.Segment
	hubServer   *hub.Server
)

func initialize(t *testing.T) {
	coordinator = createSegment(t, 1, -1, constants.RolePrimary, constants.RolePrimary, 7000, "cdw", "cdw", "/data/primary/gpseg-1")
	primary1 = createSegment(t, 2, 0, constants.RolePrimary, constants.RolePrimary, 7001, "sdw1", "sdw1", "/data/primary/gpseg0")
	primary2 = createSegment(t, 4, 1, constants.RolePrimary, constants.RolePrimary, 7003, "sdw2", "sdw2", "/data/primary/gpseg1")
	mirror1 = createSegment(t, 3, 0, constants.RoleMirror, constants.RoleMirror, 7002, "sdw2", "sdw2", "/data/mirror/gpseg0")
	mirror2 = createSegment(t, 5, 1, constants.RoleMirror, constants.RoleMirror, 7004, "sdw1", "sdw1", "/data/mirror/gpseg1")

	gparray = &greenplum.GpArray{
		Coordinator: coordinator,
		SegmentPairs: []greenplum.SegmentPair{
			{
				Primary: primary1,
				Mirror:  mirror1,
			},
			{
				Primary: primary2,
				Mirror:  mirror2,
			},
		},
	}

	credentials := &testutils.MockCredentials{TlsConnection: insecure.NewCredentials()}
	hubConfig := &hub.Config{
		1234,
		5678,
		[]string{"sdw1", "sdw2"},
		"/tmp/logDir",
		"gp",
		"gpHome",
		credentials,
	}
	hubServer = hub.New(hubConfig, nil)
}

func TestUpdatePgHbaConf(t *testing.T) {
	initialize(t)

	mirrorSegs := []*idl.Segment{
		{
			Port:          int32(mirror1.Port),
			HostName:      mirror1.Hostname,
			HostAddress:   mirror1.Address,
			DataDirectory: mirror1.DataDir,
			Contentid:     int32(mirror1.Content),
		},
		{
			Port:          int32(mirror2.Port),
			HostName:      mirror2.Hostname,
			HostAddress:   mirror2.Address,
			DataDirectory: mirror2.DataDir,
			Contentid:     int32(mirror2.Content),
		},
	}

	t.Run("succesfully updates the pg_hba.conf file when hba_hostnames is false", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		addrMap := map[string][]string{
			"sdw1": {"192.0.1.0/24"},
			"sdw2": {"192.0.2.0/24"},
		}

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().UpdatePgHbaConf(
			gomock.Any(),
			&idl.UpdatePgHbaConfRequest{
				Pgdata:      primary1.DataDir,
				Addrs:       addrMap[mirror1.Hostname],
				Replication: true,
			},
		).Return(&idl.UpdatePgHbaConfResponse{}, nil)
		sdw1.EXPECT().GetInterfaceAddrs(
			gomock.Any(),
			&idl.GetInterfaceAddrsRequest{},
		).Return(&idl.GetInterfaceAddrsResponse{
			Addrs: addrMap["sdw1"],
		}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().UpdatePgHbaConf(
			gomock.Any(),
			&idl.UpdatePgHbaConfRequest{
				Pgdata:      primary2.DataDir,
				Addrs:       addrMap[mirror2.Hostname],
				Replication: true,
			},
		).Return(&idl.UpdatePgHbaConfResponse{}, nil)
		sdw2.EXPECT().GetInterfaceAddrs(
			gomock.Any(),
			&idl.GetInterfaceAddrsRequest{},
		).Return(&idl.GetInterfaceAddrsResponse{
			Addrs: addrMap["sdw2"],
		}, nil)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		err := hubServer.UpdatePgHbaConf(gparray, mirrorSegs, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("succesfully updates the pg_hba.conf file when hba_hostnames is true", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().UpdatePgHbaConf(
			gomock.Any(),
			&idl.UpdatePgHbaConfRequest{
				Pgdata:      primary1.DataDir,
				Addrs:       []string{mirror1.Hostname},
				Replication: true,
			},
		).Return(&idl.UpdatePgHbaConfResponse{}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().UpdatePgHbaConf(
			gomock.Any(),
			&idl.UpdatePgHbaConfRequest{
				Pgdata:      primary2.DataDir,
				Addrs:       []string{mirror2.Hostname},
				Replication: true,
			},
		).Return(&idl.UpdatePgHbaConfResponse{}, nil)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		err := hubServer.UpdatePgHbaConf(gparray, mirrorSegs, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("errors out when not able to find the mirror content in gparray", func(t *testing.T) {
		segs := []*idl.Segment{{Contentid: 1234}}
		err := hubServer.UpdatePgHbaConf(gparray, segs, true)

		expectedErrString := "could not find any segments with content 1234"
		if err.Error() != expectedErrString {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}
	})

	t.Run("errors out when not able to get the interface addresses for a host", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		addrMap := map[string][]string{
			"sdw1": {"192.0.1.0/24"},
			"sdw2": {"192.0.2.0/24"},
		}
		expectedErr := errors.New("error")

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().GetInterfaceAddrs(
			gomock.Any(),
			&idl.GetInterfaceAddrsRequest{},
		).Return(&idl.GetInterfaceAddrsResponse{
			Addrs: addrMap["sdw1"],
		}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().UpdatePgHbaConf(
			gomock.Any(),
			&idl.UpdatePgHbaConfRequest{
				Pgdata:      primary2.DataDir,
				Addrs:       addrMap[mirror2.Hostname],
				Replication: true,
			},
		).Return(&idl.UpdatePgHbaConfResponse{}, nil)
		sdw2.EXPECT().GetInterfaceAddrs(
			gomock.Any(),
			&idl.GetInterfaceAddrsRequest{},
		).Return(nil, expectedErr)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		err := hubServer.UpdatePgHbaConf(gparray, mirrorSegs, false)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}

		expectedErrString := "failed to get interface addresses for host sdw2"
		if !strings.Contains(err.Error(), expectedErrString) {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}
	})

	t.Run("errors out when fails to modify the pg_hba.conf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errors.New("error")

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().UpdatePgHbaConf(
			gomock.Any(),
			&idl.UpdatePgHbaConfRequest{
				Pgdata:      primary1.DataDir,
				Addrs:       []string{mirror1.Hostname},
				Replication: true,
			},
		).Return(&idl.UpdatePgHbaConfResponse{}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().UpdatePgHbaConf(
			gomock.Any(),
			&idl.UpdatePgHbaConfRequest{
				Pgdata:      primary2.DataDir,
				Addrs:       []string{mirror2.Hostname},
				Replication: true,
			},
		).Return(nil, expectedErr)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		err := hubServer.UpdatePgHbaConf(gparray, mirrorSegs, true)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}

		expectedErrString := fmt.Sprintf("host: sdw2, %v", expectedErr)
		if err.Error() != expectedErrString {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}
	})
}

func TestValidateDataChecksums(t *testing.T) {
	testhelper.SetupTestLogger()
	initialize(t)

	t.Run("when the value is consistent across the cluster", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cdw := mock_idl.NewMockAgentClient(ctrl)
		cdw.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: coordinator.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: primary1.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)
		sdw1.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: mirror2.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: primary2.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)
		sdw2.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: mirror1.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)

		agentConns := []*hub.Connection{
			{AgentClient: cdw, Hostname: "cdw"},
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}

		err := hub.ValidateDataChecksums(agentConns, gparray)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("when the value is not consistent between the coordinator and the segments", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cdw := mock_idl.NewMockAgentClient(ctrl)
		cdw.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: coordinator.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: primary1.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)
		sdw1.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: mirror2.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: primary2.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)
		sdw2.EXPECT().PgControlData(
			gomock.Any(),
			&idl.PgControlDataRequest{
				Pgdata: mirror1.DataDir,
			},
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "0",
			},
		}, nil)

		agentConns := []*hub.Connection{
			{AgentClient: cdw, Hostname: "cdw"},
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}

		err := hub.ValidateDataChecksums(agentConns, gparray)
		expectedErrString := fmt.Sprintf("data page checksum version for segments with dbid %+v does not match the coordinator value of 1", []int{mirror1.Dbid})
		if err.Error() != expectedErrString {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}
	})

	t.Run("errors out when not able to get the coordinator value", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errors.New("error")
		cdw := mock_idl.NewMockAgentClient(ctrl)
		cdw.EXPECT().PgControlData(
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expectedErr)

		agentConns := []*hub.Connection{
			{AgentClient: cdw, Hostname: "cdw"},
		}

		err := hub.ValidateDataChecksums(agentConns, gparray)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}

		expectedErrString := fmt.Sprintf("host: cdw, %v", expectedErr)
		if err.Error() != expectedErrString {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}
	})

	t.Run("errors out when not able to get the value from the segments", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cdw := mock_idl.NewMockAgentClient(ctrl)
		cdw.EXPECT().PgControlData(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "0",
			},
		}, nil)

		expectedErr := errors.New("error")
		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().PgControlData(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "0",
			},
		}, nil)
		sdw1.EXPECT().PgControlData(
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expectedErr)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().PgControlData(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)
		sdw2.EXPECT().PgControlData(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.PgControlDataResponse{
			Result: map[string]string{
				"Data page checksum version": "1",
			},
		}, nil)

		agentConns := []*hub.Connection{
			{AgentClient: cdw, Hostname: "cdw"},
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}

		err := hub.ValidateDataChecksums(agentConns, gparray)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}

		expectedErrString := fmt.Sprintf("host: sdw1, %v", expectedErr)
		if err.Error() != expectedErrString {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}
	})
}

func TestCreateMirrorSegments(t *testing.T) {
	_, _, logfile := testhelper.SetupTestLogger()
	initialize(t)

	mirrorSegs := []*idl.Segment{
		{
			Port:          int32(mirror1.Port),
			HostName:      mirror1.Hostname,
			HostAddress:   mirror1.Address,
			DataDirectory: mirror1.DataDir,
			Contentid:     int32(mirror1.Content),
		},
		{
			Port:          int32(mirror2.Port),
			HostName:      mirror2.Hostname,
			HostAddress:   mirror2.Address,
			DataDirectory: mirror2.DataDir,
			Contentid:     int32(mirror2.Content),
		},
	}

	t.Run("successfully creates the mirror segments", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().PgBasebackup(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.PgBasebackupResponse{}, nil)
		sdw1.EXPECT().UpdatePgConf(
			gomock.Any(),
			&idl.UpdatePgConfRequest{
				Pgdata: mirror1.DataDir,
				Params: map[string]string{
					"port": strconv.Itoa(mirror1.Port),
				},
				Overwrite: true,
			},
		).Return(&idl.UpdatePgConfRespoonse{}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().PgBasebackup(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.PgBasebackupResponse{}, nil)
		sdw2.EXPECT().UpdatePgConf(
			gomock.Any(),
			&idl.UpdatePgConfRequest{
				Pgdata: mirror2.DataDir,
				Params: map[string]string{
					"port": strconv.Itoa(mirror2.Port),
				},
				Overwrite: true,
			},
		).Return(&idl.UpdatePgConfRespoonse{}, nil)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		mock, stream := testutils.NewMockStream()
		err := hubServer.CreateMirrorSegments(mock, gparray, mirrorSegs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expectedStreamResponse := make([]*idl.HubReply, len(mirrorSegs)+1)
		for i := range expectedStreamResponse {
			expectedStreamResponse[i] = &idl.HubReply{
				Message: &idl.HubReply_ProgressMsg{
					ProgressMsg: &idl.ProgressMessage{
						Label: "Initializing mirror segments:",
						Total: int32(len(mirrorSegs)),
					},
				},
			}
		}

		if !reflect.DeepEqual(stream.GetBuffer(), expectedStreamResponse) {
			t.Fatalf("got %+v, want %+v", stream.GetBuffer(), expectedStreamResponse)
		}

		testutils.AssertLogMessage(t, logfile, `\[DEBUG\]:-Starting to create mirror segment`)
		testutils.AssertLogMessage(t, logfile, `\[DEBUG\]:-Successfully ran pg_basebackup`)
		testutils.AssertLogMessage(t, logfile, `\[DEBUG\]:-Starting to modify the postgresql.conf`)
		testutils.AssertLogMessage(t, logfile, `\[DEBUG\]:-Successfully modified the postgresql.conf`)
		testutils.AssertLogMessage(t, logfile, `\[DEBUG\]:-Successfully created mirror segment`)
	})

	t.Run("errors out when fails to run pg_basebackup", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errors.New("error")
		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().PgBasebackup(
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expectedErr)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().PgBasebackup(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.PgBasebackupResponse{}, nil)
		sdw2.EXPECT().UpdatePgConf(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.UpdatePgConfRespoonse{}, nil)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		mock, stream := testutils.NewMockStream()
		err := hubServer.CreateMirrorSegments(mock, gparray, mirrorSegs)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}

		expectedErrString := fmt.Sprintf("host: sdw1, %v", expectedErr)
		if err.Error() != expectedErrString {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}

		expectedStreamResponse := make([]*idl.HubReply, len(mirrorSegs))
		for i := range expectedStreamResponse {
			expectedStreamResponse[i] = &idl.HubReply{
				Message: &idl.HubReply_ProgressMsg{
					ProgressMsg: &idl.ProgressMessage{
						Label: "Initializing mirror segments:",
						Total: int32(len(mirrorSegs)),
					},
				},
			}
		}

		if !reflect.DeepEqual(stream.GetBuffer(), expectedStreamResponse) {
			t.Fatalf("got %+v, want %+v", stream.GetBuffer(), expectedStreamResponse)
		}
	})

	t.Run("errors out when fails to update the postgresql.conf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errors.New("error")
		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().PgBasebackup(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.PgBasebackupResponse{}, nil)
		sdw1.EXPECT().UpdatePgConf(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.UpdatePgConfRespoonse{}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().PgBasebackup(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.PgBasebackupResponse{}, nil)
		sdw2.EXPECT().UpdatePgConf(
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expectedErr)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		mock, stream := testutils.NewMockStream()
		err := hubServer.CreateMirrorSegments(mock, gparray, mirrorSegs)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got %#v, want %#v", err, expectedErr)
		}

		expectedErrString := fmt.Sprintf("host: sdw2, %v", expectedErr)
		if err.Error() != expectedErrString {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}

		expectedStreamResponse := make([]*idl.HubReply, len(mirrorSegs))
		for i := range expectedStreamResponse {
			expectedStreamResponse[i] = &idl.HubReply{
				Message: &idl.HubReply_ProgressMsg{
					ProgressMsg: &idl.ProgressMessage{
						Label: "Initializing mirror segments:",
						Total: int32(len(mirrorSegs)),
					},
				},
			}
		}

		if !reflect.DeepEqual(stream.GetBuffer(), expectedStreamResponse) {
			t.Fatalf("got %+v, want %+v", stream.GetBuffer(), expectedStreamResponse)
		}
	})

	t.Run("errors out when not able to find the mirror content in gparray", func(t *testing.T) {
		segs := []*idl.Segment{{Contentid: 1234}}

		mock, stream := testutils.NewMockStream()
		err := hubServer.CreateMirrorSegments(mock, gparray, segs)

		expectedErrString := "could not find any segments with content 1234"
		if err.Error() != expectedErrString {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}

		if len(stream.GetBuffer()) != 0 {
			t.Fatalf("expected no stream responses, got %+v", stream.GetBuffer())
		}
	})
}

func TestStartMirrorSegments(t *testing.T) {
	testhelper.SetupTestLogger()
	initialize(t)

	mirrorSegs := []*idl.Segment{
		{
			Port:          int32(mirror1.Port),
			HostName:      mirror1.Hostname,
			HostAddress:   mirror1.Address,
			DataDirectory: mirror1.DataDir,
			Contentid:     int32(mirror1.Content),
		},
		{
			Port:          int32(mirror2.Port),
			HostName:      mirror2.Hostname,
			HostAddress:   mirror2.Address,
			DataDirectory: mirror2.DataDir,
			Contentid:     int32(mirror2.Content),
		},
	}

	t.Run("successfully starts the mirror segments", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().StartSegment(
			gomock.Any(),
			&idl.StartSegmentRequest{
				DataDir: mirrorSegs[1].DataDirectory,
				Wait:    true,
				Options: "-c gp_role=execute",
			},
		).Return(&idl.StartSegmentReply{}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().StartSegment(
			gomock.Any(),
			&idl.StartSegmentRequest{
				DataDir: mirrorSegs[0].DataDirectory,
				Wait:    true,
				Options: "-c gp_role=execute",
			},
		).Return(&idl.StartSegmentReply{}, nil)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		err := hubServer.StartMirrorSegments(mirrorSegs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("errors out when not able to start the segment", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().StartSegment(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.StartSegmentReply{}, nil)

		expectedErr := errors.New("error")
		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().StartSegment(
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expectedErr)

		agentConns := []*hub.Connection{
			{AgentClient: sdw1, Hostname: "sdw1"},
			{AgentClient: sdw2, Hostname: "sdw2"},
		}
		hubServer.Conns = agentConns

		err := hubServer.StartMirrorSegments(mirrorSegs)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("got%#v, want %#v", err, expectedErr)
		}

		expectedErrString := fmt.Sprintf("host: sdw2, %v", expectedErr)
		if err.Error() != expectedErrString {
			t.Fatalf("got %v, want %s", err, expectedErrString)
		}
	})
}

func createSegment(t *testing.T, dbid int, content int, role string, preferredRole string, port int, hostname string, address string, dataDir string) *greenplum.Segment {
	t.Helper()

	return &greenplum.Segment{
		Dbid:          dbid,
		Content:       content,
		Role:          role,
		PreferredRole: preferredRole,
		Port:          port,
		Hostname:      hostname,
		Address:       address,
		DataDir:       dataDir,
	}
}

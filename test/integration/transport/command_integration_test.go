package integration

import (
	"context"
	"errors"
	transport "pulsardb/internal/transport/util"
	"pulsardb/internal/types"
	"sync/atomic"
	"testing"
	"time"

	"pulsardb/internal/command"
	"pulsardb/internal/raft"
	"pulsardb/internal/storage"
	"pulsardb/internal/transport/gen/commandevents"

	"github.com/stretchr/testify/require"
)

type fakeRaftProposer struct {
	nodeID       uint64
	appliedIndex atomic.Uint64
	cmdSvc       *command.Service
}

func newFakeRaftProposer(nodeID uint64) *fakeRaftProposer {
	f := &fakeRaftProposer{nodeID: nodeID}
	f.appliedIndex.Store(100)
	return f
}

func (f *fakeRaftProposer) Propose(ctx context.Context, data []byte) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	_, err := f.cmdSvc.Apply(data)
	return err
}

func (f *fakeRaftProposer) GetReadIndex(ctx context.Context) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return f.appliedIndex.Load(), nil
}

func (f *fakeRaftProposer) GetReadIndexFromLeader(ctx context.Context, leaderID uint64) (uint64, error) {
	return f.appliedIndex.Load(), nil
}

func (f *fakeRaftProposer) WaitUntilApplied(ctx context.Context, index uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (f *fakeRaftProposer) IsLeader() bool   { return true }
func (f *fakeRaftProposer) LeaderID() uint64 { return f.nodeID }
func (f *fakeRaftProposer) NodeID() uint64   { return f.nodeID }

func (f *fakeRaftProposer) ForwardToLeader(
	ctx context.Context,
	req *commandeventspb.CommandEventRequest,
) (*commandeventspb.CommandEventResponse, error) {
	return nil, errors.New("not implemented")
}

type lazyRegistry struct {
	svc *command.Service
}

func (r *lazyRegistry) RegisterPending(eventID uint64, ch chan *commandeventspb.CommandEventResponse) {
	r.svc.RegisterPending(eventID, ch)
}

func (r *lazyRegistry) UnregisterPending(eventID uint64) {
	r.svc.UnregisterPending(eventID)
}

func startCommandPipeline(t *testing.T, nodeID uint64) (*command.Service, *storage.Service, *fakeRaftProposer) {
	t.Helper()

	storageSvc := storage.NewStorageService()
	fakeRaft := newFakeRaftProposer(nodeID)
	registry := &lazyRegistry{}

	batcher := raft.NewBatcher(fakeRaft, registry, 100, 10*time.Millisecond)
	cmdSvc := command.NewCommandService(storageSvc, fakeRaft, batcher)

	fakeRaft.cmdSvc = cmdSvc
	registry.svc = cmdSvc

	return cmdSvc, storageSvc, fakeRaft
}

func processCmd(t *testing.T, cmdSvc *command.Service, req *commandeventspb.CommandEventRequest) *commandeventspb.CommandEventResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := cmdSvc.ProcessCommand(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func requireSuccess(t *testing.T, resp *commandeventspb.CommandEventResponse) {
	t.Helper()
	require.NotNil(t, resp)
	require.True(t, resp.GetSuccess())
	require.Empty(t, resp.GetError())
}

func requireFailure(t *testing.T, resp *commandeventspb.CommandEventResponse) {
	t.Helper()
	require.NotNil(t, resp)
	require.False(t, resp.GetSuccess())
	require.NotEmpty(t, resp.GetError())
}

func TestCommand_ProcessCommand_InvalidRequests_SET_MissingKey(t *testing.T) {
	const nodeID = uint64(1)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	resp, err := cmdSvc.ProcessCommand(context.Background(), transport.BuildModifyRequest(1, commandeventspb.CommandEventType_SET, "", types.ValueToProto(1)))

	require.Error(t, err)
	requireFailure(t, resp)
}

func TestCommand_ProcessCommand_InvalidRequests_SET_MissingValue(t *testing.T) {
	const nodeID = uint64(1)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	resp, err := cmdSvc.ProcessCommand(context.Background(), transport.BuildModifyRequest(2, commandeventspb.CommandEventType_SET, "k", nil))

	require.Error(t, err)
	requireFailure(t, resp)
}

func TestCommand_ProcessCommand_InvalidRequests_GET_WithValue(t *testing.T) {
	const nodeID = uint64(1)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	resp, err := cmdSvc.ProcessCommand(context.Background(), transport.BuildModifyRequest(1, commandeventspb.CommandEventType_GET, "", types.ValueToProto(1)))

	require.Error(t, err)
	requireFailure(t, resp)
}

func TestCommand_ProcessCommand_InvalidRequests_GET_MissingKey(t *testing.T) {
	const nodeID = uint64(1)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	resp, err := cmdSvc.ProcessCommand(context.Background(), transport.BuildReadRequest(2, commandeventspb.CommandEventType_GET, ""))

	require.Error(t, err)
	requireFailure(t, resp)
}

func TestCommand_ProcessCommand_InvalidRequests_DELETE_WithValue(t *testing.T) {
	const nodeID = uint64(1)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	resp, err := cmdSvc.ProcessCommand(context.Background(), transport.BuildModifyRequest(1, commandeventspb.CommandEventType_DELETE, "", types.ValueToProto(1)))

	require.Error(t, err)
	requireFailure(t, resp)
}

func TestCommand_ProcessCommand_InvalidRequests_UnknownType(t *testing.T) {
	const nodeID = uint64(1)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	resp, err := cmdSvc.ProcessCommand(context.Background(), transport.BuildModifyRequest(3, commandeventspb.CommandEventType(999), "", types.ValueToProto(3)))

	require.Error(t, err)
	requireFailure(t, resp)
}

func TestCommand_SetThenGet_RoundTrip(t *testing.T) {
	const nodeID = uint64(7)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	setResp := processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
		EventId: 10,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "mykey",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 123},
		},
	})
	requireSuccess(t, setResp)

	getResp := processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
		EventId: 11,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "mykey",
	})
	requireSuccess(t, getResp)
	require.NotNil(t, getResp.GetValue())
	require.Equal(t, int64(123), getResp.GetValue().GetIntValue())
}

func TestCommand_DeleteThenGet_ReturnsFailure(t *testing.T) {
	const nodeID = uint64(2)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	setResp := processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
		EventId: 20,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "todelete",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "v"},
		},
	})
	requireSuccess(t, setResp)

	delResp := processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
		EventId: 21,
		Type:    commandeventspb.CommandEventType_DELETE,
		Key:     "todelete",
	})
	requireSuccess(t, delResp)

	getResp := processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
		EventId: 22,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "todelete",
	})
	requireFailure(t, getResp)
}

func TestCommand_GET_NonExistentKey_ReturnsFailure(t *testing.T) {
	const nodeID = uint64(3)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	getResp := processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
		EventId: 30,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "nonexistent",
	})

	requireFailure(t, getResp)
	require.NotNil(t, getResp.GetError())
	require.Contains(t, getResp.GetError().GetMessage(), "not found")
}

func TestCommand_OverwriteExistingKey(t *testing.T) {
	const nodeID = uint64(4)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
		EventId: 40,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "overwrite",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "first"},
		},
	})

	processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
		EventId: 41,
		Type:    commandeventspb.CommandEventType_SET,
		Key:     "overwrite",
		Value: &commandeventspb.CommandEventValue{
			Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "second"},
		},
	})

	getResp := processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
		EventId: 42,
		Type:    commandeventspb.CommandEventType_GET,
		Key:     "overwrite",
	})
	requireSuccess(t, getResp)
	require.Equal(t, "second", getResp.GetValue().GetStringValue())
}

func TestCommand_ProcessCommand_ContextCanceled_PropagatesError(t *testing.T) {
	const nodeID = uint64(9)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := cmdSvc.ProcessCommand(ctx, transport.BuildReadRequest(100, commandeventspb.CommandEventType_GET, "k"))

	require.Error(t, err)
}

func TestCommand_ProcessCommand_ContextDeadlineExceeded(t *testing.T) {
	const nodeID = uint64(10)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond)

	_, err := cmdSvc.ProcessCommand(ctx, transport.BuildReadRequest(101, commandeventspb.CommandEventType_GET, "k"))

	require.Error(t, err)
}

func TestCommand_MultipleTypes_StoredCorrectly(t *testing.T) {
	const nodeID = uint64(3)
	cmdSvc, _, _ := startCommandPipeline(t, nodeID)

	testCases := []struct {
		name     string
		key      string
		eventID  uint64
		setValue *commandeventspb.CommandEventValue
		validate func(t *testing.T, v *commandeventspb.CommandEventValue)
	}{
		{
			name:    "string",
			key:     "str-key",
			eventID: 201,
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_StringValue{StringValue: "abc"},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.Equal(t, "abc", v.GetStringValue())
			},
		},
		{
			name:    "int",
			key:     "int-key",
			eventID: 203,
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_IntValue{IntValue: 12345},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.Equal(t, int64(12345), v.GetIntValue())
			},
		},
		{
			name:    "double",
			key:     "double-key",
			eventID: 205,
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_DoubleValue{DoubleValue: 3.14159},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.InDelta(t, 3.14159, v.GetDoubleValue(), 0.00001)
			},
		},
		{
			name:    "bool",
			key:     "bool-key",
			eventID: 207,
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_BoolValue{BoolValue: true},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.True(t, v.GetBoolValue())
			},
		},
		{
			name:    "bytes",
			key:     "bytes-key",
			eventID: 209,
			setValue: &commandeventspb.CommandEventValue{
				Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
			},
			validate: func(t *testing.T, v *commandeventspb.CommandEventValue) {
				require.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, v.GetBytesValue())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setResp := processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
				EventId: tc.eventID,
				Type:    commandeventspb.CommandEventType_SET,
				Key:     tc.key,
				Value:   tc.setValue,
			})
			requireSuccess(t, setResp)

			getResp := processCmd(t, cmdSvc, &commandeventspb.CommandEventRequest{
				EventId: tc.eventID + 1,
				Type:    commandeventspb.CommandEventType_GET,
				Key:     tc.key,
			})
			requireSuccess(t, getResp)
			tc.validate(t, getResp.GetValue())
		})
	}
}

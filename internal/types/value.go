package types

import (
	"fmt"
	snapshotpb "pulsardb/internal/raft/gen"
	"pulsardb/internal/transport/gen/commandevents"
)

func ValueFromProto(v *commandeventspb.CommandEventValue) any {
	if v == nil {
		return nil
	}
	switch val := v.Value.(type) {
	case *commandeventspb.CommandEventValue_StringValue:
		return val.StringValue
	case *commandeventspb.CommandEventValue_IntValue:
		return val.IntValue
	case *commandeventspb.CommandEventValue_DoubleValue:
		return val.DoubleValue
	case *commandeventspb.CommandEventValue_BoolValue:
		return val.BoolValue
	case *commandeventspb.CommandEventValue_BytesValue:
		return val.BytesValue
	default:
		return nil
	}
}

func ValueToProto(v any) *commandeventspb.CommandEventValue {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case string:
		return &commandeventspb.CommandEventValue{Value: &commandeventspb.CommandEventValue_StringValue{StringValue: val}}
	case int64:
		return &commandeventspb.CommandEventValue{Value: &commandeventspb.CommandEventValue_IntValue{IntValue: val}}
	case int:
		return &commandeventspb.CommandEventValue{Value: &commandeventspb.CommandEventValue_IntValue{IntValue: int64(val)}}
	case float64:
		return &commandeventspb.CommandEventValue{Value: &commandeventspb.CommandEventValue_DoubleValue{DoubleValue: val}}
	case bool:
		return &commandeventspb.CommandEventValue{Value: &commandeventspb.CommandEventValue_BoolValue{BoolValue: val}}
	case []byte:
		return &commandeventspb.CommandEventValue{Value: &commandeventspb.CommandEventValue_BytesValue{BytesValue: val}}
	default:
		return nil
	}
}

func ToProtoValue(v any) (*snapshotpb.Value, error) {
	switch val := v.(type) {
	case string:
		return &snapshotpb.Value{Kind: &snapshotpb.Value_StringValue{StringValue: val}}, nil
	case int64:
		return &snapshotpb.Value{Kind: &snapshotpb.Value_Int64Value{Int64Value: val}}, nil
	case int:
		return &snapshotpb.Value{Kind: &snapshotpb.Value_Int64Value{Int64Value: int64(val)}}, nil
	case float64:
		return &snapshotpb.Value{Kind: &snapshotpb.Value_Float64Value{Float64Value: val}}, nil
	case bool:
		return &snapshotpb.Value{Kind: &snapshotpb.Value_BoolValue{BoolValue: val}}, nil
	case []byte:
		return &snapshotpb.Value{Kind: &snapshotpb.Value_BytesValue{BytesValue: val}}, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func FromProtoValue(v *snapshotpb.Value) any {
	if v == nil {
		return nil
	}
	switch k := v.Kind.(type) {
	case *snapshotpb.Value_StringValue:
		return k.StringValue
	case *snapshotpb.Value_Int64Value:
		return k.Int64Value
	case *snapshotpb.Value_Float64Value:
		return k.Float64Value
	case *snapshotpb.Value_BoolValue:
		return k.BoolValue
	case *snapshotpb.Value_BytesValue:
		return k.BytesValue
	default:
		return nil
	}
}

package command

import (
	commandevents "pulsardb/internal/transport/gen"
)

// ValueFromProto converts a proto CommandEventValue to a Go value.
func ValueFromProto(v *commandevents.CommandEventValue) any {
	if v == nil {
		return nil
	}
	switch val := v.GetValue().(type) {
	case *commandevents.CommandEventValue_StringValue:
		return val.StringValue
	case *commandevents.CommandEventValue_IntValue:
		return val.IntValue
	case *commandevents.CommandEventValue_DoubleValue:
		return val.DoubleValue
	case *commandevents.CommandEventValue_BoolValue:
		return val.BoolValue
	case *commandevents.CommandEventValue_BytesValue:
		return val.BytesValue
	default:
		return nil
	}
}

// ValueToProto converts a Go value to a proto CommandEventValue.
func ValueToProto(a any) *commandevents.CommandEventValue {
	switch v := a.(type) {
	case string:
		return &commandevents.CommandEventValue{
			Value: &commandevents.CommandEventValue_StringValue{StringValue: v},
		}
	case int:
		return &commandevents.CommandEventValue{
			Value: &commandevents.CommandEventValue_IntValue{IntValue: int64(v)},
		}
	case int64:
		return &commandevents.CommandEventValue{
			Value: &commandevents.CommandEventValue_IntValue{IntValue: v},
		}
	case float64:
		return &commandevents.CommandEventValue{
			Value: &commandevents.CommandEventValue_DoubleValue{DoubleValue: v},
		}
	case bool:
		return &commandevents.CommandEventValue{
			Value: &commandevents.CommandEventValue_BoolValue{BoolValue: v},
		}
	case []byte:
		return &commandevents.CommandEventValue{
			Value: &commandevents.CommandEventValue_BytesValue{BytesValue: v},
		}
	default:
		return nil
	}
}

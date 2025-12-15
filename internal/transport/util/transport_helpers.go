package transport

import (
	"pulsardb/internal/transport/gen/commandevents"
)

func SuccessReadResponse(eventID uint64, val *commandeventspb.CommandEventValue) *commandeventspb.CommandEventResponse {
	return &commandeventspb.CommandEventResponse{
		Success: true,
		EventId: eventID,
		Value:   val,
	}
}

func SuccessModifyResponse(eventID uint64) *commandeventspb.CommandEventResponse {
	return &commandeventspb.CommandEventResponse{
		Success: true,
		EventId: eventID,
	}
}

func ErrorResponse(eventID uint64, code commandeventspb.ErrorCode, msg string) *commandeventspb.CommandEventResponse {
	return &commandeventspb.CommandEventResponse{
		EventId: eventID,
		Success: false,
		Error: &commandeventspb.CommandError{
			Code:    code,
			Message: msg,
		},
	}
}

func BuildReadRequest(eventId uint64, requestType commandeventspb.CommandEventType, key string) *commandeventspb.CommandEventRequest {
	return &commandeventspb.CommandEventRequest{
		EventId: eventId,
		Type:    requestType,
		Key:     key,
	}
}

func BuildModifyRequest(eventId uint64, requestType commandeventspb.CommandEventType, key string, val *commandeventspb.CommandEventValue) *commandeventspb.CommandEventRequest {
	return &commandeventspb.CommandEventRequest{
		EventId: eventId,
		Type:    requestType,
		Key:     key,
		Value:   val,
	}
}

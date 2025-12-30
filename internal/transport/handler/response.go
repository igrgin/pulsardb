package handler

import "pulsardb/internal/transport/gen/commandevents"

func SuccessRead(eventID uint64, val *commandeventspb.CommandEventValue) *commandeventspb.CommandEventResponse {
	return &commandeventspb.CommandEventResponse{
		EventId: eventID,
		Success: true,
		Value:   val,
	}
}

func SuccessWrite(eventID uint64) *commandeventspb.CommandEventResponse {
	return &commandeventspb.CommandEventResponse{
		EventId: eventID,
		Success: true,
	}
}

func Error(eventID uint64, code commandeventspb.ErrorCode, msg string) *commandeventspb.CommandEventResponse {
	return &commandeventspb.CommandEventResponse{
		EventId: eventID,
		Success: false,
		Error: &commandeventspb.CommandError{
			Code:    code,
			Message: msg,
		},
	}
}

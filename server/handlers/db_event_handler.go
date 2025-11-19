package handlers

import (
	"context"
	"pulsardb/server/gen"
)

type DBEventServer struct {
	db_events.UnimplementedDBEventServiceServer
}

func (*DBEventServer) HandleEvent(context context.Context, request *db_events.DBEventRequest) (*db_events.DBEventResponse, error) {
	message := "Hello"
	return &db_events.DBEventResponse{Success: false, ErrorMessage: message}, nil
}

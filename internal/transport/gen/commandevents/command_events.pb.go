package commandeventspb

import (
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type CommandEventType int32

const (
	CommandEventType_SET    CommandEventType = 0
	CommandEventType_GET    CommandEventType = 1
	CommandEventType_DELETE CommandEventType = 2
)

// Enum value maps for CommandEventType.
var (
	CommandEventType_name = map[int32]string{
		0: "SET",
		1: "GET",
		2: "DELETE",
	}
	CommandEventType_value = map[string]int32{
		"SET":    0,
		"GET":    1,
		"DELETE": 2,
	}
)

func (x CommandEventType) Enum() *CommandEventType {
	p := new(CommandEventType)
	*p = x
	return p
}

func (x CommandEventType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (CommandEventType) Descriptor() protoreflect.EnumDescriptor {
	return file_command_events_proto_enumTypes[0].Descriptor()
}

func (CommandEventType) Type() protoreflect.EnumType {
	return &file_command_events_proto_enumTypes[0]
}

func (x CommandEventType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use CommandEventType.Descriptor instead.
func (CommandEventType) EnumDescriptor() ([]byte, []int) {
	return file_command_events_proto_rawDescGZIP(), []int{0}
}

type ErrorCode int32

const (
	ErrorCode_UNKNOWN         ErrorCode = 0
	ErrorCode_KEY_NOT_FOUND   ErrorCode = 1
	ErrorCode_INVALID_REQUEST ErrorCode = 2
	ErrorCode_NOT_LEADER      ErrorCode = 3
	ErrorCode_NO_QUORUM       ErrorCode = 4
	ErrorCode_TIMEOUT         ErrorCode = 5
)

// Enum value maps for ErrorCode.
var (
	ErrorCode_name = map[int32]string{
		0: "UNKNOWN",
		1: "KEY_NOT_FOUND",
		2: "INVALID_REQUEST",
		3: "NOT_LEADER",
		4: "NO_QUORUM",
		5: "TIMEOUT",
	}
	ErrorCode_value = map[string]int32{
		"UNKNOWN":         0,
		"KEY_NOT_FOUND":   1,
		"INVALID_REQUEST": 2,
		"NOT_LEADER":      3,
		"NO_QUORUM":       4,
		"TIMEOUT":         5,
	}
)

func (x ErrorCode) Enum() *ErrorCode {
	p := new(ErrorCode)
	*p = x
	return p
}

func (x ErrorCode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ErrorCode) Descriptor() protoreflect.EnumDescriptor {
	return file_command_events_proto_enumTypes[1].Descriptor()
}

func (ErrorCode) Type() protoreflect.EnumType {
	return &file_command_events_proto_enumTypes[1]
}

func (x ErrorCode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ErrorCode.Descriptor instead.
func (ErrorCode) EnumDescriptor() ([]byte, []int) {
	return file_command_events_proto_rawDescGZIP(), []int{1}
}

type CommandEventValue struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Value:
	//
	//	*CommandEventValue_StringValue
	//	*CommandEventValue_IntValue
	//	*CommandEventValue_DoubleValue
	//	*CommandEventValue_BoolValue
	//	*CommandEventValue_BytesValue
	Value         isCommandEventValue_Value `protobuf_oneof:"value"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CommandEventValue) Reset() {
	*x = CommandEventValue{}
	mi := &file_command_events_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CommandEventValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommandEventValue) ProtoMessage() {}

func (x *CommandEventValue) ProtoReflect() protoreflect.Message {
	mi := &file_command_events_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommandEventValue.ProtoReflect.Descriptor instead.
func (*CommandEventValue) Descriptor() ([]byte, []int) {
	return file_command_events_proto_rawDescGZIP(), []int{0}
}

func (x *CommandEventValue) GetValue() isCommandEventValue_Value {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *CommandEventValue) GetStringValue() string {
	if x != nil {
		if x, ok := x.Value.(*CommandEventValue_StringValue); ok {
			return x.StringValue
		}
	}
	return ""
}

func (x *CommandEventValue) GetIntValue() int64 {
	if x != nil {
		if x, ok := x.Value.(*CommandEventValue_IntValue); ok {
			return x.IntValue
		}
	}
	return 0
}

func (x *CommandEventValue) GetDoubleValue() float64 {
	if x != nil {
		if x, ok := x.Value.(*CommandEventValue_DoubleValue); ok {
			return x.DoubleValue
		}
	}
	return 0
}

func (x *CommandEventValue) GetBoolValue() bool {
	if x != nil {
		if x, ok := x.Value.(*CommandEventValue_BoolValue); ok {
			return x.BoolValue
		}
	}
	return false
}

func (x *CommandEventValue) GetBytesValue() []byte {
	if x != nil {
		if x, ok := x.Value.(*CommandEventValue_BytesValue); ok {
			return x.BytesValue
		}
	}
	return nil
}

type isCommandEventValue_Value interface {
	isCommandEventValue_Value()
}

type CommandEventValue_StringValue struct {
	StringValue string `protobuf:"bytes,1,opt,name=string_value,json=stringValue,proto3,oneof"`
}

type CommandEventValue_IntValue struct {
	IntValue int64 `protobuf:"varint,2,opt,name=int_value,json=intValue,proto3,oneof"`
}

type CommandEventValue_DoubleValue struct {
	DoubleValue float64 `protobuf:"fixed64,3,opt,name=double_value,json=doubleValue,proto3,oneof"`
}

type CommandEventValue_BoolValue struct {
	BoolValue bool `protobuf:"varint,4,opt,name=bool_value,json=boolValue,proto3,oneof"`
}

type CommandEventValue_BytesValue struct {
	BytesValue []byte `protobuf:"bytes,5,opt,name=bytes_value,json=bytesValue,proto3,oneof"`
}

func (*CommandEventValue_StringValue) isCommandEventValue_Value() {}

func (*CommandEventValue_IntValue) isCommandEventValue_Value() {}

func (*CommandEventValue_DoubleValue) isCommandEventValue_Value() {}

func (*CommandEventValue_BoolValue) isCommandEventValue_Value() {}

func (*CommandEventValue_BytesValue) isCommandEventValue_Value() {}

type CommandEventRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	EventId       uint64                 `protobuf:"varint,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	Type          CommandEventType       `protobuf:"varint,2,opt,name=type,proto3,enum=events.CommandEventType" json:"type,omitempty"`
	Key           string                 `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	Value         *CommandEventValue     `protobuf:"bytes,4,opt,name=value,proto3" json:"value,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CommandEventRequest) Reset() {
	*x = CommandEventRequest{}
	mi := &file_command_events_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CommandEventRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommandEventRequest) ProtoMessage() {}

func (x *CommandEventRequest) ProtoReflect() protoreflect.Message {
	mi := &file_command_events_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommandEventRequest.ProtoReflect.Descriptor instead.
func (*CommandEventRequest) Descriptor() ([]byte, []int) {
	return file_command_events_proto_rawDescGZIP(), []int{1}
}

func (x *CommandEventRequest) GetEventId() uint64 {
	if x != nil {
		return x.EventId
	}
	return 0
}

func (x *CommandEventRequest) GetType() CommandEventType {
	if x != nil {
		return x.Type
	}
	return CommandEventType_SET
}

func (x *CommandEventRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *CommandEventRequest) GetValue() *CommandEventValue {
	if x != nil {
		return x.Value
	}
	return nil
}

type CommandEventResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	EventId       uint64                 `protobuf:"varint,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	Success       bool                   `protobuf:"varint,3,opt,name=success,proto3" json:"success,omitempty"`
	Value         *CommandEventValue     `protobuf:"bytes,4,opt,name=value,proto3" json:"value,omitempty"`
	Error         *CommandError          `protobuf:"bytes,5,opt,name=error,proto3" json:"error,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CommandEventResponse) Reset() {
	*x = CommandEventResponse{}
	mi := &file_command_events_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CommandEventResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommandEventResponse) ProtoMessage() {}

func (x *CommandEventResponse) ProtoReflect() protoreflect.Message {
	mi := &file_command_events_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommandEventResponse.ProtoReflect.Descriptor instead.
func (*CommandEventResponse) Descriptor() ([]byte, []int) {
	return file_command_events_proto_rawDescGZIP(), []int{2}
}

func (x *CommandEventResponse) GetEventId() uint64 {
	if x != nil {
		return x.EventId
	}
	return 0
}

func (x *CommandEventResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *CommandEventResponse) GetValue() *CommandEventValue {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *CommandEventResponse) GetError() *CommandError {
	if x != nil {
		return x.Error
	}
	return nil
}

type CommandError struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Code          ErrorCode              `protobuf:"varint,1,opt,name=code,proto3,enum=events.ErrorCode" json:"code,omitempty"`
	Message       string                 `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CommandError) Reset() {
	*x = CommandError{}
	mi := &file_command_events_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CommandError) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommandError) ProtoMessage() {}

func (x *CommandError) ProtoReflect() protoreflect.Message {
	mi := &file_command_events_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommandError.ProtoReflect.Descriptor instead.
func (*CommandError) Descriptor() ([]byte, []int) {
	return file_command_events_proto_rawDescGZIP(), []int{3}
}

func (x *CommandError) GetCode() ErrorCode {
	if x != nil {
		return x.Code
	}
	return ErrorCode_UNKNOWN
}

func (x *CommandError) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

type BatchedCommands struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Commands      []*CommandEventRequest `protobuf:"bytes,1,rep,name=commands,proto3" json:"commands,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *BatchedCommands) Reset() {
	*x = BatchedCommands{}
	mi := &file_command_events_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BatchedCommands) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchedCommands) ProtoMessage() {}

func (x *BatchedCommands) ProtoReflect() protoreflect.Message {
	mi := &file_command_events_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchedCommands.ProtoReflect.Descriptor instead.
func (*BatchedCommands) Descriptor() ([]byte, []int) {
	return file_command_events_proto_rawDescGZIP(), []int{4}
}

func (x *BatchedCommands) GetCommands() []*CommandEventRequest {
	if x != nil {
		return x.Commands
	}
	return nil
}

var File_command_events_proto protoreflect.FileDescriptor

const file_command_events_proto_rawDesc = "" +
	"\n" +
	"\x14command_events.proto\x12\x06events\"\xc9\x01\n" +
	"\x11CommandEventValue\x12#\n" +
	"\fstring_value\x18\x01 \x01(\tH\x00R\vstringValue\x12\x1d\n" +
	"\tint_value\x18\x02 \x01(\x03H\x00R\bintValue\x12#\n" +
	"\fdouble_value\x18\x03 \x01(\x01H\x00R\vdoubleValue\x12\x1f\n" +
	"\n" +
	"bool_value\x18\x04 \x01(\bH\x00R\tboolValue\x12!\n" +
	"\vbytes_value\x18\x05 \x01(\fH\x00R\n" +
	"bytesValueB\a\n" +
	"\x05value\"\xa1\x01\n" +
	"\x13CommandEventRequest\x12\x19\n" +
	"\bevent_id\x18\x01 \x01(\x04R\aeventId\x12,\n" +
	"\x04type\x18\x02 \x01(\x0e2\x18.events.CommandEventTypeR\x04type\x12\x10\n" +
	"\x03key\x18\x03 \x01(\tR\x03key\x12/\n" +
	"\x05value\x18\x04 \x01(\v2\x19.events.CommandEventValueR\x05value\"\xa8\x01\n" +
	"\x14CommandEventResponse\x12\x19\n" +
	"\bevent_id\x18\x01 \x01(\x04R\aeventId\x12\x18\n" +
	"\asuccess\x18\x03 \x01(\bR\asuccess\x12/\n" +
	"\x05value\x18\x04 \x01(\v2\x19.events.CommandEventValueR\x05value\x12*\n" +
	"\x05error\x18\x05 \x01(\v2\x14.events.CommandErrorR\x05error\"O\n" +
	"\fCommandError\x12%\n" +
	"\x04code\x18\x01 \x01(\x0e2\x11.events.ErrorCodeR\x04code\x12\x18\n" +
	"\amessage\x18\x02 \x01(\tR\amessage\"J\n" +
	"\x0fBatchedCommands\x127\n" +
	"\bcommands\x18\x01 \x03(\v2\x1b.events.CommandEventRequestR\bcommands*0\n" +
	"\x10CommandEventType\x12\a\n" +
	"\x03SET\x10\x00\x12\a\n" +
	"\x03GET\x10\x01\x12\n" +
	"\n" +
	"\x06DELETE\x10\x02*l\n" +
	"\tErrorCode\x12\v\n" +
	"\aUNKNOWN\x10\x00\x12\x11\n" +
	"\rKEY_NOT_FOUND\x10\x01\x12\x13\n" +
	"\x0fINVALID_REQUEST\x10\x02\x12\x0e\n" +
	"\n" +
	"NOT_LEADER\x10\x03\x12\r\n" +
	"\tNO_QUORUM\x10\x04\x12\v\n" +
	"\aTIMEOUT\x10\x052m\n" +
	"\x19CommandEventClientService\x12P\n" +
	"\x13ProcessCommandEvent\x12\x1b.events.CommandEventRequest\x1a\x1c.events.CommandEventResponseBNZLgithub.com/you/pulsardb/internal/transport/gen/commandevents;commandeventspbb\x06proto3"

var (
	file_command_events_proto_rawDescOnce sync.Once
	file_command_events_proto_rawDescData []byte
)

func file_command_events_proto_rawDescGZIP() []byte {
	file_command_events_proto_rawDescOnce.Do(func() {
		file_command_events_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_command_events_proto_rawDesc), len(file_command_events_proto_rawDesc)))
	})
	return file_command_events_proto_rawDescData
}

var file_command_events_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_command_events_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_command_events_proto_goTypes = []any{
	(CommandEventType)(0),        // 0: events.CommandEventType
	(ErrorCode)(0),               // 1: events.ErrorCode
	(*CommandEventValue)(nil),    // 2: events.CommandEventValue
	(*CommandEventRequest)(nil),  // 3: events.CommandEventRequest
	(*CommandEventResponse)(nil), // 4: events.CommandEventResponse
	(*CommandError)(nil),         // 5: events.CommandError
	(*BatchedCommands)(nil),      // 6: events.BatchedCommands
}
var file_command_events_proto_depIdxs = []int32{
	0, // 0: events.CommandEventRequest.type:type_name -> events.CommandEventType
	2, // 1: events.CommandEventRequest.value:type_name -> events.CommandEventValue
	2, // 2: events.CommandEventResponse.value:type_name -> events.CommandEventValue
	5, // 3: events.CommandEventResponse.error:type_name -> events.CommandError
	1, // 4: events.CommandError.code:type_name -> events.ErrorCode
	3, // 5: events.BatchedCommands.commands:type_name -> events.CommandEventRequest
	3, // 6: events.CommandEventClientService.ProcessCommandEvent:input_type -> events.CommandEventRequest
	4, // 7: events.CommandEventClientService.ProcessCommandEvent:output_type -> events.CommandEventResponse
	7, // [7:8] is the sub-list for method output_type
	6, // [6:7] is the sub-list for method input_type
	6, // [6:6] is the sub-list for extension type_name
	6, // [6:6] is the sub-list for extension extendee
	0, // [0:6] is the sub-list for field type_name
}

func init() { file_command_events_proto_init() }
func file_command_events_proto_init() {
	if File_command_events_proto != nil {
		return
	}
	file_command_events_proto_msgTypes[0].OneofWrappers = []any{
		(*CommandEventValue_StringValue)(nil),
		(*CommandEventValue_IntValue)(nil),
		(*CommandEventValue_DoubleValue)(nil),
		(*CommandEventValue_BoolValue)(nil),
		(*CommandEventValue_BytesValue)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_command_events_proto_rawDesc), len(file_command_events_proto_rawDesc)),
			NumEnums:      2,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_command_events_proto_goTypes,
		DependencyIndexes: file_command_events_proto_depIdxs,
		EnumInfos:         file_command_events_proto_enumTypes,
		MessageInfos:      file_command_events_proto_msgTypes,
	}.Build()
	File_command_events_proto = out.File
	file_command_events_proto_goTypes = nil
	file_command_events_proto_depIdxs = nil
}

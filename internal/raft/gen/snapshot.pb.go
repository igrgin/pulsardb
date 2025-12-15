package snapshot

import (
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)

	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type KeyValue struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Key           string                 `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value         *Value                 `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *KeyValue) Reset() {
	*x = KeyValue{}
	mi := &file_snapshot_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *KeyValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeyValue) ProtoMessage() {}

func (x *KeyValue) ProtoReflect() protoreflect.Message {
	mi := &file_snapshot_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*KeyValue) Descriptor() ([]byte, []int) {
	return file_snapshot_proto_rawDescGZIP(), []int{0}
}

func (x *KeyValue) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *KeyValue) GetValue() *Value {
	if x != nil {
		return x.Value
	}
	return nil
}

type Value struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Kind          isValue_Kind           `protobuf_oneof:"kind"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Value) Reset() {
	*x = Value{}
	mi := &file_snapshot_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Value) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Value) ProtoMessage() {}

func (x *Value) ProtoReflect() protoreflect.Message {
	mi := &file_snapshot_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*Value) Descriptor() ([]byte, []int) {
	return file_snapshot_proto_rawDescGZIP(), []int{1}
}

func (x *Value) GetKind() isValue_Kind {
	if x != nil {
		return x.Kind
	}
	return nil
}

func (x *Value) GetStringValue() string {
	if x != nil {
		if x, ok := x.Kind.(*Value_StringValue); ok {
			return x.StringValue
		}
	}
	return ""
}

func (x *Value) GetInt64Value() int64 {
	if x != nil {
		if x, ok := x.Kind.(*Value_Int64Value); ok {
			return x.Int64Value
		}
	}
	return 0
}

func (x *Value) GetFloat64Value() float64 {
	if x != nil {
		if x, ok := x.Kind.(*Value_Float64Value); ok {
			return x.Float64Value
		}
	}
	return 0
}

func (x *Value) GetBoolValue() bool {
	if x != nil {
		if x, ok := x.Kind.(*Value_BoolValue); ok {
			return x.BoolValue
		}
	}
	return false
}

func (x *Value) GetBytesValue() []byte {
	if x != nil {
		if x, ok := x.Kind.(*Value_BytesValue); ok {
			return x.BytesValue
		}
	}
	return nil
}

type isValue_Kind interface {
	isValue_Kind()
}

type Value_StringValue struct {
	StringValue string `protobuf:"bytes,1,opt,name=string_value,json=stringValue,proto3,oneof"`
}

type Value_Int64Value struct {
	Int64Value int64 `protobuf:"varint,2,opt,name=int64_value,json=int64Value,proto3,oneof"`
}

type Value_Float64Value struct {
	Float64Value float64 `protobuf:"fixed64,3,opt,name=float64_value,json=float64Value,proto3,oneof"`
}

type Value_BoolValue struct {
	BoolValue bool `protobuf:"varint,4,opt,name=bool_value,json=boolValue,proto3,oneof"`
}

type Value_BytesValue struct {
	BytesValue []byte `protobuf:"bytes,5,opt,name=bytes_value,json=bytesValue,proto3,oneof"`
}

func (*Value_StringValue) isValue_Kind() {}

func (*Value_Int64Value) isValue_Kind() {}

func (*Value_Float64Value) isValue_Kind() {}

func (*Value_BoolValue) isValue_Kind() {}

func (*Value_BytesValue) isValue_Kind() {}

type KVSnapshot struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Entries       []*KeyValue            `protobuf:"bytes,1,rep,name=entries,proto3" json:"entries,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *KVSnapshot) Reset() {
	*x = KVSnapshot{}
	mi := &file_snapshot_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *KVSnapshot) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KVSnapshot) ProtoMessage() {}

func (x *KVSnapshot) ProtoReflect() protoreflect.Message {
	mi := &file_snapshot_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*KVSnapshot) Descriptor() ([]byte, []int) {
	return file_snapshot_proto_rawDescGZIP(), []int{2}
}

func (x *KVSnapshot) GetEntries() []*KeyValue {
	if x != nil {
		return x.Entries
	}
	return nil
}

var File_snapshot_proto protoreflect.FileDescriptor

const file_snapshot_proto_rawDesc = "" +
	"\n" +
	"\x0esnapshot.proto\x12\bsnapshot\"C\n" +
	"\bKeyValue\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12%\n" +
	"\x05value\x18\x02 \x01(\v2\x0f.snapshot.ValueR\x05value\"\xc2\x01\n" +
	"\x05Value\x12#\n" +
	"\fstring_value\x18\x01 \x01(\tH\x00R\vstringValue\x12!\n" +
	"\vint64_value\x18\x02 \x01(\x03H\x00R\n" +
	"int64Value\x12%\n" +
	"\rfloat64_value\x18\x03 \x01(\x01H\x00R\ffloat64Value\x12\x1f\n" +
	"\n" +
	"bool_value\x18\x04 \x01(\bH\x00R\tboolValue\x12!\n" +
	"\vbytes_value\x18\x05 \x01(\fH\x00R\n" +
	"bytesValueB\x06\n" +
	"\x04kind\":\n" +
	"\n" +
	"KVSnapshot\x12,\n" +
	"\aentries\x18\x01 \x03(\v2\x12.snapshot.KeyValueR\aentriesB*Z(pulsardb/internal/transport/gen/snapshotb\x06proto3"

var (
	file_snapshot_proto_rawDescOnce sync.Once
	file_snapshot_proto_rawDescData []byte
)

func file_snapshot_proto_rawDescGZIP() []byte {
	file_snapshot_proto_rawDescOnce.Do(func() {
		file_snapshot_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_snapshot_proto_rawDesc), len(file_snapshot_proto_rawDesc)))
	})
	return file_snapshot_proto_rawDescData
}

var file_snapshot_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_snapshot_proto_goTypes = []any{
	(*KeyValue)(nil),
	(*Value)(nil),
	(*KVSnapshot)(nil),
}
var file_snapshot_proto_depIdxs = []int32{
	1,
	0,
	2,
	2,
	2,
	2,
	0,
}

func init() { file_snapshot_proto_init() }
func file_snapshot_proto_init() {
	if File_snapshot_proto != nil {
		return
	}
	file_snapshot_proto_msgTypes[1].OneofWrappers = []any{
		(*Value_StringValue)(nil),
		(*Value_Int64Value)(nil),
		(*Value_Float64Value)(nil),
		(*Value_BoolValue)(nil),
		(*Value_BytesValue)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_snapshot_proto_rawDesc), len(file_snapshot_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_snapshot_proto_goTypes,
		DependencyIndexes: file_snapshot_proto_depIdxs,
		MessageInfos:      file_snapshot_proto_msgTypes,
	}.Build()
	File_snapshot_proto = out.File
	file_snapshot_proto_goTypes = nil
	file_snapshot_proto_depIdxs = nil
}

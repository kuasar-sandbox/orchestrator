package execadmission

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	requestMessageName = protoreflect.FullName("kuasar.exec.admission.ExecRequest")
	stdioMessageName   = protoreflect.FullName("kuasar.exec.admission.Stdio")
)

type requestTypes struct {
	requestType protoreflect.MessageType
	stdioType   protoreflect.MessageType

	argv, env, cwd, user, stdio protoreflect.FieldDescriptor
	tty, stdin, stdout, stderr  protoreflect.FieldDescriptor
}

func newRequestTypes() (*requestTypes, error) {
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	boolType := descriptorpb.FieldDescriptorProto_TYPE_BOOL
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Syntax:  proto.String("proto3"),
		Name:    proto.String("kuasar/exec/admission/request.proto"),
		Package: proto.String("kuasar.exec.admission"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Stdio"),
				Field: []*descriptorpb.FieldDescriptorProto{
					field("tty", 1, optional, boolType, ""),
					field("stdin", 2, optional, boolType, ""),
					field("stdout", 3, optional, boolType, ""),
					field("stderr", 4, optional, boolType, ""),
				},
			},
			{
				Name: proto.String("ExecRequest"),
				NestedType: []*descriptorpb.DescriptorProto{{
					Name:    proto.String("EnvEntry"),
					Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					Field: []*descriptorpb.FieldDescriptorProto{
						field("key", 1, optional, stringType, ""),
						field("value", 2, optional, stringType, ""),
					},
				}},
				Field: []*descriptorpb.FieldDescriptorProto{
					field("argv", 1, repeated, stringType, ""),
					field("env", 2, repeated, messageType, ".kuasar.exec.admission.ExecRequest.EnvEntry"),
					field("cwd", 3, optional, stringType, ""),
					field("user", 4, optional, stringType, ""),
					field("stdio", 5, optional, messageType, ".kuasar.exec.admission.Stdio"),
				},
			},
		},
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("build exec admission descriptor: %w", err)
	}
	requestDescriptor := file.Messages().ByName("ExecRequest")
	stdioDescriptor := file.Messages().ByName("Stdio")
	if requestDescriptor == nil || stdioDescriptor == nil {
		return nil, fmt.Errorf("build exec admission descriptor: missing messages")
	}
	fields := requestDescriptor.Fields()
	stdioFields := stdioDescriptor.Fields()
	return &requestTypes{
		requestType: dynamicpb.NewMessageType(requestDescriptor),
		stdioType:   dynamicpb.NewMessageType(stdioDescriptor),
		argv:        fields.ByName("argv"), env: fields.ByName("env"),
		cwd: fields.ByName("cwd"), user: fields.ByName("user"), stdio: fields.ByName("stdio"),
		tty: stdioFields.ByName("tty"), stdin: stdioFields.ByName("stdin"),
		stdout: stdioFields.ByName("stdout"), stderr: stdioFields.ByName("stderr"),
	}, nil
}

func field(
	name string,
	number int32,
	label descriptorpb.FieldDescriptorProto_Label,
	kind descriptorpb.FieldDescriptorProto_Type,
	typeName string,
) *descriptorpb.FieldDescriptorProto {
	out := &descriptorpb.FieldDescriptorProto{
		Name: proto.String(name), Number: proto.Int32(number),
		Label: &label, Type: &kind,
	}
	if typeName != "" {
		out.TypeName = proto.String(typeName)
	}
	return out
}

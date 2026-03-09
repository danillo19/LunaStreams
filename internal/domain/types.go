package domain

type OperationKind string

const (
	OperationKindSource    OperationKind = "source"
	OperationKindTransform OperationKind = "transform"
)

type ExecutionMode string

const (
	ExecutionModeAlwaysOn     ExecutionMode = "always_on"
	ExecutionModeTaskPerEvent ExecutionMode = "task_per_event"
)

type StreamType string

const (
	StreamTypeBinary     StreamType = "binary"
	StreamTypeAudioChunk StreamType = "audio_chunk"
	StreamTypeBool       StreamType = "bool"
)

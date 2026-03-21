package ir

type OperationKind string

const (
	OperationKindSource    OperationKind = "source"
	OperationKindTransform OperationKind = "transform"
	OperationKindSelector  OperationKind = "selector"
	OperationKindSink      OperationKind = "sink"
)

type OperationMode string

const (
	OperationModeAlwaysOn     OperationMode = "always_on"
	OperationModeTaskPerEvent OperationMode = "task_per_event"
)

type StreamType string

const (
	StreamTypeVideoFrame StreamType = "video_frame"
	StreamTypeAudioChunk StreamType = "audio_chunk"
	StreamTypeBool       StreamType = "bool"
	StreamTypeText       StreamType = "text"
)

type Document struct {
	Operations []Operation `yaml:"operations"`
	Streams    []Stream    `yaml:"streams"`
}

type Operation struct {
	ID      string          `yaml:"id"`
	Kind    OperationKind   `yaml:"kind"`
	Mode    OperationMode   `yaml:"mode"`
	Impl    string          `yaml:"impl"`
	Inputs  []StreamRef     `yaml:"inputs"`
	Outputs []StreamRef     `yaml:"outputs"`
	Config  map[string]any  `yaml:"config"`
	Domain  OperationDomain `yaml:"domain"`
}

type OperationDomain struct {
	Cost   *float64 `yaml:"cost"`
	Weight *float64 `yaml:"weight"`
}

type StreamRef struct {
	Stream string `yaml:"stream"`
}

type Stream struct {
	ID   string     `yaml:"id"`
	Type StreamType `yaml:"type"`
}

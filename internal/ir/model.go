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
	Model        ModelSpec   `yaml:"model"`
	Task         TaskSpec    `yaml:"task"`
	TaskVariants []TaskSpec  `yaml:"task_variants"`
	Operations   []Operation `yaml:"operations"`
	Streams      []Stream    `yaml:"streams"`
}

type ModelSpec struct {
	Operations []Operation `yaml:"operations"`
	Streams    []Stream    `yaml:"streams"`
}

type TaskSpec struct {
	ID                string             `yaml:"id"`
	Inputs            []TaskStreamRef    `yaml:"inputs"`
	Outputs           []TaskStreamRef    `yaml:"outputs"`
	OperationProfiles []OperationProfile `yaml:"operation_profiles"`
	Constraints       TaskConstraints    `yaml:"constraints"`
	Objective         TaskObjective      `yaml:"objective"`
	Selectors         []SelectorTask     `yaml:"selectors"`
}

type TaskConstraints struct {
	MinTotalWeight    *float64 `yaml:"min_total_weight"`
	MaxTotalLatencyMS *int     `yaml:"max_total_latency_ms"`
	MaxTotalCost      *float64 `yaml:"max_total_cost"`
}

type TaskObjective struct {
	Primary   string `yaml:"primary"`
	Secondary string `yaml:"secondary"`
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

type TaskStreamRef struct {
	Stream string `yaml:"stream"`
}

type OperationProfile struct {
	Operation string   `yaml:"operation"`
	Cost      *float64 `yaml:"cost"`
	Weight    *float64 `yaml:"weight"`
	LatencyMS *int     `yaml:"latency_ms"`
}

type SelectorTask struct {
	ID                       string            `yaml:"id"`
	SelectionMode            string            `yaml:"selection_mode"`
	DefaultChoice            string            `yaml:"default_choice"`
	SelectionWeightThreshold *float64          `yaml:"selection_weight_threshold"`
	MaxTotalLatencyMS        *int              `yaml:"max_total_latency_ms"`
	Variants                 []SelectorVariant `yaml:"variants"`
}

type SelectorVariant struct {
	Stream    string   `yaml:"stream"`
	Kind      string   `yaml:"kind"`
	Label     string   `yaml:"label"`
	WindowMS  *int     `yaml:"window_ms"`
	Cost      *float64 `yaml:"cost"`
	Weight    *float64 `yaml:"weight"`
	LatencyMS *int     `yaml:"latency_ms"`
}

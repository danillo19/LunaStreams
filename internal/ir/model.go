package ir

type OperationKind string

const (
	OperationKindSource    OperationKind = "source"
	OperationKindTransform OperationKind = "transform"
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
	OperationConfigs  []OperationConfig  `yaml:"operation_configs"`
	Constraints       TaskConstraints    `yaml:"constraints"`
	Objective         TaskObjective      `yaml:"objective"`
	Selectors         []SelectorTask     `yaml:"selectors"`
}

// OperationConfig — набор параметров, которые задача подставляет в
// конкретную операцию модели. Модель описывает только структуру
// (kind/impl/runtime/inputs/outputs), а конкретные значения (пороги,
// порты, размеры буферов, selector-variants и т.п.) приходят из задачи.
//
// Merge-семантика: ключи из Config поверх op.Config операции модели;
// отсутствующие ключи сохраняются. Повторное упоминание того же id
// в пределах одной задачи — ошибка валидации.
type OperationConfig struct {
	Operation string         `yaml:"operation"`
	Config    map[string]any `yaml:"config"`
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
	Runtime RuntimeSpec     `yaml:"runtime"`
	Inputs  []StreamRef     `yaml:"inputs"`
	Outputs []StreamRef     `yaml:"outputs"`
	Config  map[string]any  `yaml:"config"`
	Domain  OperationDomain `yaml:"domain"`
}

// RuntimeSpec описывает способ запуска операции: в процессе Go, во внешнем
// процессе (Python/Node/Rust), в Docker-контейнере, через gRPC и т.п.
// Пустой Kind эквивалентен "inproc_go" и включает обратную совместимость со
// старыми IR, где операция реализована фабрикой в Go.
type RuntimeSpec struct {
	Kind     string            `yaml:"kind"`
	Command  []string          `yaml:"command"`
	Args     []string          `yaml:"args"`
	Env      map[string]string `yaml:"env"`
	WorkDir  string            `yaml:"work_dir"`
	Image    string            `yaml:"image"`
	Endpoint string            `yaml:"endpoint"`
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

// TaskStreamRef — элемент task.inputs / task.outputs.
//
// Для inputs допустим только Stream — задача описывает, какие данные она
// считает входными. Для outputs элемент может нести либо Stream (нужный
// доменный результат), либо Sink (требуемый побочный эффект: консоль,
// веб-UI, файл и т.п.). Планировщик трактует оба случая одинаково:
// собирает граф так, чтобы указанные streams были вычислены, а
// указанные sinks — запущены (и их inputs подтянулись автоматически).
// Ровно одно из полей должно быть непустым; одновременно задавать оба
// нельзя — валидация это отсекает.
type TaskStreamRef struct {
	Stream string `yaml:"stream"`
	Sink   string `yaml:"sink"`
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

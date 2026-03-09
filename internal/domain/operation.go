package domain

type IR struct {
	Operations []Operation `yaml:"operations" json:"operations"`
	Streams    []Stream    `yaml:"streams" json:"streams"`
}

type Operation struct {
	ID      string         `yaml:"id" json:"id"`
	Kind    OperationKind  `yaml:"kind" json:"kind"`
	Mode    ExecutionMode  `yaml:"mode" json:"mode"`
	ImplURL string         `yaml:"impl" json:"impl"`
	Inputs  []Stream       `yaml:"inputs,omitempty" json:"inputs,omitempty"`
	Outputs []Stream       `yaml:"outputs,omitempty" json:"outputs,omitempty"`
	Config  map[string]any `yaml:"config,omitempty" json:"config,omitempty"`
}

type Stream struct {
	ID   string     `yaml:"id" json:"id"`
	Type StreamType `yaml:"type" json:"type"`
}

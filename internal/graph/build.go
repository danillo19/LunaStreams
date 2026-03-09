package graph

import (
	"fmt"

	"LunaStreams/internal/ir"
)

type Graph struct {
	Operations        map[string]ir.Operation
	ProducerByStream  map[string]string
	ConsumersByStream map[string][]string
}

func Build(doc *ir.Document) (*Graph, error) {
	if doc == nil {
		return nil, fmt.Errorf("ir document is nil")
	}

	g := &Graph{
		Operations:        make(map[string]ir.Operation, len(doc.Operations)),
		ProducerByStream:  make(map[string]string, len(doc.Streams)),
		ConsumersByStream: make(map[string][]string, len(doc.Streams)),
	}

	for _, op := range doc.Operations {
		g.Operations[op.ID] = op

		for _, output := range op.Outputs {
			if producer, exists := g.ProducerByStream[output.Stream]; exists {
				return nil, fmt.Errorf("stream %q already produced by %q", output.Stream, producer)
			}
			g.ProducerByStream[output.Stream] = op.ID
		}

		for _, input := range op.Inputs {
			g.ConsumersByStream[input.Stream] = append(g.ConsumersByStream[input.Stream], op.ID)
		}
	}

	return g, nil
}

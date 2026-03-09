package runtime

import (
	"context"
	"fmt"
	"sync"

	"LunaStreams/internal/ir"
)

type Operator interface {
	Run(ctx context.Context, inputs map[string]any) (map[string]any, error)
}

type ClosableOperator interface {
	Close() error
}

type Factory func(spec ir.Operation) (Operator, error)

type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]Factory),
	}
}

func (r *Registry) Register(impl string, factory Factory) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if impl == "" {
		return fmt.Errorf("impl must not be empty")
	}
	if factory == nil {
		return fmt.Errorf("factory for %q is nil", impl)
	}
	if _, exists := r.factories[impl]; exists {
		return fmt.Errorf("impl %q already registered", impl)
	}

	r.factories[impl] = factory
	return nil
}

func (r *Registry) Has(impl string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.factories[impl]
	return exists
}

func (r *Registry) Create(spec ir.Operation) (Operator, error) {
	r.mu.RLock()
	factory, exists := r.factories[spec.Impl]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("impl %q is not registered", spec.Impl)
	}

	operator, err := factory(spec)
	if err != nil {
		return nil, fmt.Errorf("create operator %q (%s): %w", spec.ID, spec.Impl, err)
	}

	return operator, nil
}

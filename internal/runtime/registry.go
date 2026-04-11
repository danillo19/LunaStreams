package runtime

import (
	"context"
	"fmt"
	"sync"

	"LunaStreams/internal/ir"
)

// Operator это минимальный контракт одной операции в runtime.
// Реализации могут быть как Go-объектами (inproc_go), так и адаптерами к
// внешним процессам, Docker-контейнерам или gRPC-сервисам.
type Operator interface {
	Run(ctx context.Context, inputs map[string]any) (map[string]any, error)
}

// ClosableOperator опционален: runtime вызовет Close() при shutdown, чтобы
// корректно освободить ресурсы (процессы, сокеты, устройства).
type ClosableOperator interface {
	Close() error
}

// Factory создаёт inproc_go операцию по её IR-спецификации.
type Factory func(spec ir.Operation) (Operator, error)

// OperationRuntime это драйвер запуска операций конкретного kind.
// Через драйверы можно подключать реализации на других языках, контейнерах
// и удалённых сервисах, не меняя remained engine и IR.
type OperationRuntime interface {
	Create(spec ir.Operation) (Operator, error)
}

// RuntimeKindInprocGo — дефолтный kind, использующий Go-фабрики.
const RuntimeKindInprocGo = "inproc_go"

// Registry связывает IR и конкретные реализации операций.
// Он хранит:
//   - map строковых impl -> Go factory (для inproc_go драйвера);
//   - map runtime.kind -> OperationRuntime драйвер.
//
// Регистрация драйверов отдельно от фабрик позволяет добавлять новые среды
// исполнения (subprocess/docker/grpc/http/...) без пересборки существующих
// операций.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
	drivers   map[string]OperationRuntime
}

func NewRegistry() *Registry {
	registry := &Registry{
		factories: make(map[string]Factory),
		drivers:   make(map[string]OperationRuntime),
	}
	registry.drivers[RuntimeKindInprocGo] = inprocDriver{registry: registry}
	return registry
}

// Register регистрирует Go-фабрику под конкретным impl.
// Используется inproc_go драйвером. Операции других runtime kind-ов не
// обязаны иметь Go-фабрику.
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

// RegisterDriver добавляет новый runtime-драйвер для заданного kind.
// Пример: RegisterDriver("subprocess", subprocess.NewDriver()).
func (r *Registry) RegisterDriver(kind string, driver OperationRuntime) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if kind == "" {
		return fmt.Errorf("runtime kind must not be empty")
	}
	if driver == nil {
		return fmt.Errorf("driver for kind %q is nil", kind)
	}
	if _, exists := r.drivers[kind]; exists {
		return fmt.Errorf("runtime kind %q already registered", kind)
	}

	r.drivers[kind] = driver
	return nil
}

// Supports сообщает, может ли registry инстанцировать указанную операцию.
// Для inproc_go требуется зарегистрированная Go-фабрика; для остальных
// kind-ов достаточно зарегистрированного драйвера.
func (r *Registry) Supports(op ir.Operation) bool {
	kind := runtimeKind(op)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, exists := r.drivers[kind]; !exists {
		return false
	}
	if kind == RuntimeKindInprocGo {
		_, exists := r.factories[op.Impl]
		return exists
	}
	return true
}

// Create выбирает драйвер по runtime.kind и делегирует ему создание оператора.
func (r *Registry) Create(spec ir.Operation) (Operator, error) {
	kind := runtimeKind(spec)

	r.mu.RLock()
	driver, exists := r.drivers[kind]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("runtime kind %q is not registered (operation %q, impl %q)", kind, spec.ID, spec.Impl)
	}

	operator, err := driver.Create(spec)
	if err != nil {
		return nil, fmt.Errorf("create operator %q (%s, runtime=%s): %w", spec.ID, spec.Impl, kind, err)
	}

	return operator, nil
}

func runtimeKind(op ir.Operation) string {
	if op.Runtime.Kind == "" {
		return RuntimeKindInprocGo
	}
	return op.Runtime.Kind
}

// inprocDriver реализует OperationRuntime поверх map фабрик Registry.
type inprocDriver struct {
	registry *Registry
}

func (d inprocDriver) Create(spec ir.Operation) (Operator, error) {
	d.registry.mu.RLock()
	factory, exists := d.registry.factories[spec.Impl]
	d.registry.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("impl %q is not registered as inproc_go factory", spec.Impl)
	}

	return factory(spec)
}

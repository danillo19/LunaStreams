// Package ops — общая точка сборки реализаций операций LunaStreams.
//
// Дерево ops/ разложено по driver/языку исполнения:
//
//	ops/
//	  inprocgo/    — Go-native операции (runtime.kind = inproc_go)
//	  subprocess/  — subprocess-драйвер + polyglot-скрипты (Python и др.)
//
// Каждый подпакет сам регистрирует себя в общем runtime.Registry через
// функцию Register(registry). Этот пакет лишь собирает их вместе через
// RegisterAll, чтобы приложение получило единую точку инициализации.
//
// Добавить новый driver (docker/grpc/http/...) — достаточно создать новый
// подпакет ops/<driver>/ с функцией Register(registry) и подключить его
// ниже. Существующие операции и engine при этом менять не нужно.
package ops

import (
	rt "LunaStreams/internal/runtime"
	"LunaStreams/ops/inprocgo"
	"LunaStreams/ops/subprocess"
)

// RegisterAll регистрирует все известные операции и runtime-драйверы в
// указанном Registry. Порядок не важен, так как inproc_go-драйвер создаётся
// Registry автоматически, а остальные драйверы регистрируются независимо.
func RegisterAll(registry *rt.Registry) error {
	if err := inprocgo.Register(registry); err != nil {
		return err
	}
	if err := subprocess.Register(registry); err != nil {
		return err
	}
	return nil
}

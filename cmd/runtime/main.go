package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"LunaStreams/internal/graph"
	"LunaStreams/internal/ir"
	"LunaStreams/internal/planner"
	rt "LunaStreams/internal/runtime"
	"LunaStreams/internal/runtime/ops"
)

func main() {
	irPath := flag.String("ir", "./examples/presence.yaml", "path to IR yaml")
	taskID := flag.String("task", "", "task id from task/task_variants")
	debug := flag.Bool("debug", false, "enable debug logs")
	flag.Parse()

	logger := rt.NewBeautifulLogger(*debug)
	rt.SetDefaultLogger(logger)

	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		log.Fatalf("register operators: %v", err)
	}

	doc, err := ir.Load(*irPath)
	if err != nil {
		log.Fatalf("load ir: %v", err)
	}
	if err := ir.ResolveTask(doc, *taskID); err != nil {
		log.Fatalf("resolve task: %v", err)
	}

	if err := ir.Validate(doc, registry); err != nil {
		log.Fatalf("validate ir: %v", err)
	}

	planResult, err := planner.CompileRedundantChoices(doc)
	if err != nil || planResult == nil {
		log.Fatalf("compile redundant choices: %v", err)
	}

	if planResult.Document != nil {
		doc = planResult.Document
	}
	for _, decision := range planResult.Decisions {
		logger.Info("planner", "selector %s planned strategy=%s disabled_ops=%v", decision.SelectorID, decision.Strategy, decision.DisabledOperationIDs)
	}

	if err := ir.Validate(doc, registry); err != nil {
		log.Fatalf("validate planned ir: %v", err)
	}

	dependencyGraph, err := graph.Build(doc)
	if err != nil {
		log.Fatalf("build graph: %v", err)
	}

	engine, err := rt.NewEngine(doc, dependencyGraph, registry, logger)
	if err != nil {
		log.Fatalf("create engine: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := engine.Start(ctx); err != nil {
		log.Fatalf("start engine: %v", err)
	}

	logger.Info("main", "runtime started with %d operations", len(doc.Operations))
	<-ctx.Done()
	logger.Info("main", "runtime stopped")
}

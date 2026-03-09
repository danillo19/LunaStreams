package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"LunaStreams/internal/graph"
	"LunaStreams/internal/ir"
	rt "LunaStreams/internal/runtime"
	"LunaStreams/internal/runtime/ops"
)

func main() {
	irPath := flag.String("ir", "./examples/presence.yaml", "path to IR yaml")
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

	if err := ir.Validate(doc, registry); err != nil {
		log.Fatalf("validate ir: %v", err)
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

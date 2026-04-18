package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"LunaStreams/internal/ir"
	"LunaStreams/internal/planner"
	rt "LunaStreams/internal/runtime"
	"LunaStreams/ops"
)

func main() {
	irDir := flag.String("ir", "examples/presence", "path to presence example directory (merged YAML)")
	outDir := flag.String("out", "examples/presence/graphs", "output directory for .mmd files")
	flag.Parse()

	absIr, err := filepath.Abs(*irDir)
	if err != nil {
		exitErr("abs ir: %v", err)
	}
	absOut, err := filepath.Abs(*outDir)
	if err != nil {
		exitErr("abs out: %v", err)
	}

	if err := os.MkdirAll(absOut, 0o755); err != nil {
		exitErr("mkdir out: %v", err)
	}

	registry := rt.NewRegistry()
	if err := ops.RegisterAll(registry); err != nil {
		exitErr("RegisterAll: %v", err)
	}

	baseDoc, err := ir.Load(absIr)
	if err != nil {
		exitErr("Load: %v", err)
	}

	if err := writeModelMermaid(absOut, baseDoc); err != nil {
		exitErr("model graph: %v", err)
	}

	taskIDs := collectTaskIDs(baseDoc)
	sort.Strings(taskIDs)
	for _, id := range taskIDs {
		if err := writeTaskMermaid(absOut, absIr, registry, id); err != nil {
			exitErr("task %q: %v", id, err)
		}
	}

	fmt.Fprintf(os.Stderr, "wrote model + %d task diagrams (.mmd) to %s\n", len(taskIDs), absOut)
	fmt.Fprintf(os.Stderr, "render PNG/SVG + validate: make graphs\n")
}

func exitErr(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func collectTaskIDs(doc *ir.Document) []string {
	seen := make(map[string]struct{})
	var ids []string
	if doc.Task.ID != "" {
		seen[doc.Task.ID] = struct{}{}
		ids = append(ids, doc.Task.ID)
	}
	for _, v := range doc.TaskVariants {
		if v.ID == "" {
			continue
		}
		if _, ok := seen[v.ID]; ok {
			continue
		}
		seen[v.ID] = struct{}{}
		ids = append(ids, v.ID)
	}
	return ids
}

func writeModelMermaid(outDir string, doc *ir.Document) error {
	mmd := buildMermaid(mermaidParams{
		comment:   "presence — model (all operations)",
		doc:       doc,
		allActive: true,
	})
	path := filepath.Join(outDir, "model.mmd")
	return os.WriteFile(path, []byte(mmd), 0o644)
}

func writeTaskMermaid(outDir, irDir string, registry *rt.Registry, taskID string) error {
	doc, err := ir.Load(irDir)
	if err != nil {
		return err
	}
	if err := ir.ResolveTask(doc, taskID); err != nil {
		return err
	}
	if err := ir.Validate(doc, registry); err != nil {
		return fmt.Errorf("validate before plan: %w", err)
	}
	plan, err := planner.CompileRedundantChoices(doc)
	if err != nil {
		return fmt.Errorf("plan: %w", err)
	}
	planned := plan.Document
	if err := ir.Validate(planned, registry); err != nil {
		return fmt.Errorf("validate planned: %w", err)
	}

	activeOps := make(map[string]struct{}, len(planned.Operations))
	for _, op := range planned.Operations {
		activeOps[op.ID] = struct{}{}
	}
	activeStreams := make(map[string]struct{}, len(planned.Streams))
	for _, s := range planned.Streams {
		activeStreams[s.ID] = struct{}{}
	}

	taskStreams := make(map[string]struct{})
	taskSinks := make(map[string]struct{})
	for _, o := range doc.Task.Outputs {
		if o.Stream != "" {
			taskStreams[o.Stream] = struct{}{}
		}
		if o.Sink != "" {
			taskSinks[o.Sink] = struct{}{}
		}
	}
	for _, in := range doc.Task.Inputs {
		if in.Stream != "" {
			taskStreams[in.Stream] = struct{}{}
		}
	}

	mmd := buildMermaid(mermaidParams{
		comment:     fmt.Sprintf("presence — task %q (green = in plan; thick orange stroke = task inputs/outputs)", taskID),
		doc:         doc,
		allActive:   false,
		activeOps:   activeOps,
		activeStr:   activeStreams,
		taskStreams: taskStreams,
		taskSinks:   taskSinks,
	})
	path := filepath.Join(outDir, "task_"+safeFileName(taskID)+".mmd")
	return os.WriteFile(path, []byte(mmd), 0o644)
}

type mermaidParams struct {
	comment     string
	doc         *ir.Document
	allActive   bool
	activeOps   map[string]struct{}
	activeStr   map[string]struct{}
	taskStreams map[string]struct{}
	taskSinks   map[string]struct{}
}

func buildMermaid(p mermaidParams) string {
	var b strings.Builder
	b.WriteString("%% Auto-generated: go run ./cmd/gen-graphs/\n")
	b.WriteString("%% " + mermaidEscapeOneLine(p.comment) + "\n")
	b.WriteString("flowchart LR\n")

	streams := append([]ir.Stream(nil), p.doc.Streams...)
	sort.Slice(streams, func(i, j int) bool { return streams[i].ID < streams[j].ID })
	for _, s := range streams {
		fmt.Fprintf(&b, "    %s[\"%s\"]\n",
			streamMermaidID(s.ID),
			mermaidBracketLabel(s.ID, string(s.Type)))
	}

	ops := append([]ir.Operation(nil), p.doc.Operations...)
	sort.Slice(ops, func(i, j int) bool { return ops[i].ID < ops[j].ID })
	for _, op := range ops {
		fmt.Fprintf(&b, "    %s[\"%s\"]\n",
			opMermaidID(op.ID),
			mermaidBracketLabel(op.ID, string(op.Kind)))
	}

	for _, op := range p.doc.Operations {
		for _, in := range op.Inputs {
			b.WriteString(fmt.Sprintf("    %s --> %s\n", streamMermaidID(in.Stream), opMermaidID(op.ID)))
		}
		for _, out := range op.Outputs {
			b.WriteString(fmt.Sprintf("    %s --> %s\n", opMermaidID(op.ID), streamMermaidID(out.Stream)))
		}
	}

	if p.allActive {
		b.WriteString(`
    classDef streamNode fill:#f6f0ff,stroke:#6b46c1,stroke-width:1px,color:#24124d;
    classDef sourceOp fill:#e3f2fd,stroke:#1565c0,stroke-width:1px,color:#0d47a1;
    classDef transformOp fill:#fff8e8,stroke:#b7791f,stroke-width:1px,color:#3b2a12;
    classDef sinkOp fill:#fce4ec,stroke:#c2185b,stroke-width:1px,color:#880e4f;
`)
		b.WriteString(classifyModelNodes(streams, ops))
		return b.String()
	}

	b.WriteString(`
    classDef dim fill:#eeeeee,stroke:#bdbdbd,stroke-width:1px,color:#9e9e9e;
    classDef streamActive fill:#e8f5e9,stroke:#2e7d32,stroke-width:1px,color:#1b5e20;
    classDef streamHot fill:#c8e6c9,stroke:#e65100,stroke-width:3px,color:#1b5e20;
    classDef opActive fill:#fff8e8,stroke:#2e7d32,stroke-width:1px,color:#3b2a12;
    classDef opHot fill:#fff8e8,stroke:#e65100,stroke-width:3px,color:#3b2a12;
`)
	b.WriteString(classifyTaskNodes(streams, ops, p))
	return b.String()
}

func classifyModelNodes(streams []ir.Stream, ops []ir.Operation) string {
	var b strings.Builder
	var streamIDs, sources, transforms, sinks []string
	for _, s := range streams {
		streamIDs = append(streamIDs, streamMermaidID(s.ID))
	}
	for _, op := range ops {
		switch op.Kind {
		case ir.OperationKindSource:
			sources = append(sources, opMermaidID(op.ID))
		case ir.OperationKindSink:
			sinks = append(sinks, opMermaidID(op.ID))
		default:
			transforms = append(transforms, opMermaidID(op.ID))
		}
	}
	if len(streamIDs) > 0 {
		b.WriteString("    class " + strings.Join(streamIDs, ",") + " streamNode;\n")
	}
	if len(sources) > 0 {
		b.WriteString("    class " + strings.Join(sources, ",") + " sourceOp;\n")
	}
	if len(transforms) > 0 {
		b.WriteString("    class " + strings.Join(transforms, ",") + " transformOp;\n")
	}
	if len(sinks) > 0 {
		b.WriteString("    class " + strings.Join(sinks, ",") + " sinkOp;\n")
	}
	return b.String()
}

func classifyTaskNodes(streams []ir.Stream, ops []ir.Operation, p mermaidParams) string {
	var dimS, activeS, hotS, dimO, activeO, hotO []string
	for _, s := range streams {
		id := streamMermaidID(s.ID)
		inPlan := mapHas(p.activeStr, s.ID)
		hot := mapHas(p.taskStreams, s.ID)
		switch {
		case !inPlan:
			dimS = append(dimS, id)
		case inPlan && hot:
			hotS = append(hotS, id)
		case inPlan:
			activeS = append(activeS, id)
		default:
			dimS = append(dimS, id)
		}
	}
	for _, op := range ops {
		id := opMermaidID(op.ID)
		inPlan := mapHas(p.activeOps, op.ID)
		hot := mapHas(p.taskSinks, op.ID)
		switch {
		case !inPlan:
			dimO = append(dimO, id)
		case inPlan && hot:
			hotO = append(hotO, id)
		case inPlan:
			activeO = append(activeO, id)
		default:
			dimO = append(dimO, id)
		}
	}
	var b strings.Builder
	writeClassLine(&b, dimS, "dim")
	writeClassLine(&b, activeS, "streamActive")
	writeClassLine(&b, hotS, "streamHot")
	writeClassLine(&b, dimO, "dim")
	writeClassLine(&b, activeO, "opActive")
	writeClassLine(&b, hotO, "opHot")
	return b.String()
}

func writeClassLine(b *strings.Builder, ids []string, class string) {
	if len(ids) == 0 {
		return
	}
	sort.Strings(ids)
	b.WriteString("    class " + strings.Join(ids, ",") + " " + class + ";\n")
}

func mapHas(m map[string]struct{}, key string) bool {
	if m == nil {
		return false
	}
	_, ok := m[key]
	return ok
}

func mermaidEscapeOneLine(s string) string {
	s = strings.ReplaceAll(s, `"`, "'")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

// mermaidBracketLabel builds text for id["..."] (HTML line breaks, no raw quotes).
func mermaidBracketLabel(title, subtitle string) string {
	title = strings.ReplaceAll(title, `"`, "'")
	subtitle = strings.ReplaceAll(subtitle, `"`, "'")
	return title + "<br/>(" + subtitle + ")"
}

func streamMermaidID(id string) string {
	return "st_" + sanitizeMermaidID(id)
}

func opMermaidID(id string) string {
	return "op_" + sanitizeMermaidID(id)
}

func sanitizeMermaidID(id string) string {
	id = strings.ReplaceAll(id, "-", "_")
	id = strings.ReplaceAll(id, ".", "_")
	return id
}

func safeFileName(id string) string {
	id = strings.ReplaceAll(id, string(filepath.Separator), "_")
	id = strings.ReplaceAll(id, "..", "_")
	return id
}

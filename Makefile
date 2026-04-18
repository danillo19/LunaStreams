PUPPETEER_EXECUTABLE_PATH ?= /Applications/Google Chrome.app/Contents/MacOS/Google Chrome
MERMAID_CLI = npx --yes @mermaid-js/mermaid-cli
MERMAID_ENV = PUPPETEER_EXECUTABLE_PATH="$(PUPPETEER_EXECUTABLE_PATH)"

MMD_DOCS = $(wildcard docs/diagrams/*.mmd)
MMD_PRESENCE = $(wildcard examples/presence/graphs/*.mmd)
MMD_ALL = $(MMD_DOCS) $(MMD_PRESENCE)

SVG_ALL = $(MMD_ALL:.mmd=.svg)
PNG_ALL = $(MMD_ALL:.mmd=.png)
VALIDATE_STAMPS = $(patsubst %.mmd,.tmp/diagram-check/%.ok,$(MMD_ALL))

.PHONY: graphs graphs-mmd graphs-validate graphs-render clean-graphs

# Full pipeline: generate Mermaid for presence, validate syntax, render assets.
graphs: graphs-render

# Generates only .mmd for examples/presence based on model+tasks.
graphs-mmd:
	go run ./cmd/gen-graphs/ -ir ./examples/presence -out ./examples/presence/graphs

# Mermaid syntax validation (plus generator validation of model/tasks/planner).
graphs-validate: graphs-mmd $(VALIDATE_STAMPS)

# Final SVG/PNG generation.
graphs-render: graphs-validate $(SVG_ALL) $(PNG_ALL)

.tmp/diagram-check/%.ok: %.mmd
	@mkdir -p "$(dir $@)" "$(dir .tmp/diagram-check/$*.svg)"
	@$(MERMAID_ENV) $(MERMAID_CLI) -i "$<" -o ".tmp/diagram-check/$*.svg" >/dev/null
	@touch "$@"

%.svg: %.mmd
	$(MERMAID_ENV) $(MERMAID_CLI) -i "$<" -o "$@"

%.png: %.mmd
	$(MERMAID_ENV) $(MERMAID_CLI) -i "$<" -o "$@"

clean-graphs:
	rm -f $(SVG_ALL) $(PNG_ALL)
	rm -rf .tmp/diagram-check

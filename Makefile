.PHONY: build lint fmt tidy test cover clean help setup-hooks

help:
	@echo "Valid targets:"
	@echo "  build       - Format, lint, and build retina-orchestrator binary"
	@echo "  lint        - Format code and run linters"
	@echo "  fmt         - Format code"
	@echo "  tidy        - Tidy go modules"
	@echo "  test        - Run tests with race detection and generate coverage profile"
	@echo "  cover       - View test coverage in browser"
	@echo "  clean       - Remove built binaries and coverage files"
	@echo "  setup-hooks - Configure local Git hooks for commit validation"

build: lint
	go build -o retina-orchestrator .

lint: fmt
	golangci-lint run

fmt:
	go fmt ./...

tidy:
	go mod tidy

test:
	go test -v -race -coverprofile=coverage.out ./...

cover:
	go tool cover -html=coverage.out

clean:
	rm -f retina-orchestrator coverage.out

setup-hooks:
	@mkdir -p .githooks
	@git config core.hooksPath .githooks
	@echo "✅ Local Git hooks configured successfully!"

# Makefile for Soyuz

.PHONY: all test clean lint fmt install-hook

VERSION ?= $(shell ./scripts/get_version.sh)
export VERSION

all: install-hook lint test

install-hook:
	@if [ -d .git ] && [ ! -L .git/hooks/pre-commit ] && [ ! -f .git/hooks/pre-commit ]; then \
		echo "Installing git pre-commit hook (symlink)..."; \
		ln -sf ../../scripts/git-pre-commit.sh .git/hooks/pre-commit; \
		fi

test:
	@echo "Running tests..."
	go test -v -race ./...

clean:
	@echo "Cleaning temporary files..."

lint:
	@echo "Running go vet..."
	go vet ./...
	@if command -v golangci-lint >/dev/null 2>&1; then \
		echo "Running golangci-lint..."; \
		golangci-lint run ./...; \
	fi

fmt:
	@echo "Formatting source code..."
	gofmt -s -w .

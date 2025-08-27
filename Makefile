# Makefile for Predixy Redis Cluster Proxy

# 变量定义
APP_NAME := predixy
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "v1.0.0")
BUILD_TIME := $(shell date -u '+%Y-%m-%d %H:%M:%S UTC')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Go 相关变量
GO := go
GOFLAGS := -ldflags "-X 'main.Version=$(VERSION)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)'"
BIN_DIR := bin
CMD_DIR := cmd/predixy

# 默认目标
.PHONY: all
all: build

# 构建
.PHONY: build
build: clean
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(APP_NAME) ./$(CMD_DIR)
	@echo "Build completed: $(BIN_DIR)/$(APP_NAME)"

# 清理
.PHONY: clean
clean:
	@echo "Cleaning..."
	@rm -rf $(BIN_DIR)
	@$(GO) clean

# 运行
.PHONY: run
run: build
	@echo "Running $(APP_NAME)..."
	./$(BIN_DIR)/$(APP_NAME)

# 开发模式运行
.PHONY: dev
dev:
	@echo "Running in development mode..."
	$(GO) run ./$(CMD_DIR) -log-level debug

# 测试
.PHONY: test
test:
	@echo "Running tests..."
	$(GO) test -v ./...

# 测试覆盖率
.PHONY: test-coverage
test-coverage:
	@echo "Running tests with coverage..."
	$(GO) test -v -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# 代码格式化
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

# 代码检查
.PHONY: lint
lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found, installing..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run

# 依赖管理
.PHONY: deps
deps:
	@echo "Downloading dependencies..."
	$(GO) mod download
	$(GO) mod tidy

# 更新依赖
.PHONY: deps-update
deps-update:
	@echo "Updating dependencies..."
	$(GO) get -u ./...
	$(GO) mod tidy

# 安装
.PHONY: install
install: build
	@echo "Installing $(APP_NAME)..."
	$(GO) install $(GOFLAGS) ./$(CMD_DIR)

# 交叉编译
.PHONY: build-all
build-all: clean
	@echo "Cross-compiling for multiple platforms..."
	@mkdir -p $(BIN_DIR)
	# Linux AMD64
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(APP_NAME)-linux-amd64 ./$(CMD_DIR)
	# Linux ARM64
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(APP_NAME)-linux-arm64 ./$(CMD_DIR)
	# macOS AMD64
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(APP_NAME)-darwin-amd64 ./$(CMD_DIR)
	# macOS ARM64
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(APP_NAME)-darwin-arm64 ./$(CMD_DIR)
	# Windows AMD64
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO) build $(GOFLAGS) -o $(BIN_DIR)/$(APP_NAME)-windows-amd64.exe ./$(CMD_DIR)
	@echo "Cross-compilation completed"

# Docker 构建
.PHONY: docker-build
docker-build:
	@echo "Building Docker image..."
	docker build -t $(APP_NAME):$(VERSION) .
	docker tag $(APP_NAME):$(VERSION) $(APP_NAME):latest

# 性能测试
.PHONY: bench
bench:
	@echo "Running benchmarks..."
	$(GO) test -bench=. -benchmem ./...

# 生成文档
.PHONY: docs
docs:
	@echo "Generating documentation..."
	@which godoc > /dev/null || (echo "godoc not found, installing..." && go install golang.org/x/tools/cmd/godoc@latest)
	godoc -http=:6060

# 版本信息
.PHONY: version
version:
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"

# 帮助信息
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build        - Build the application"
	@echo "  clean        - Clean build artifacts"
	@echo "  run          - Build and run the application"
	@echo "  dev          - Run in development mode"
	@echo "  test         - Run tests"
	@echo "  test-coverage- Run tests with coverage"
	@echo "  fmt          - Format code"
	@echo "  lint         - Run linter"
	@echo "  deps         - Download dependencies"
	@echo "  deps-update  - Update dependencies"
	@echo "  install      - Install the application"
	@echo "  build-all    - Cross-compile for multiple platforms"
	@echo "  docker-build - Build Docker image"
	@echo "  bench        - Run benchmarks"
	@echo "  docs         - Generate documentation"
	@echo "  version      - Show version information"
	@echo "  help         - Show this help message"
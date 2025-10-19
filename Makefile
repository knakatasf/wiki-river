# ----- config
PROTO_DIR := internal/proto
PROTO_SRC := $(wildcard $(PROTO_DIR)/*.proto)

# ----- tools
GOBIN := $(shell go env GOPATH)/bin
PROTOC_GEN_GO := $(GOBIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(GOBIN)/protoc-gen-go-grpc

# ----- targets
.PHONY: all build proto tidy run-local clean

all: build

proto: $(PROTO_SRC)
	@[ -x "$(PROTOC_GEN_GO)" ] || (echo "Missing protoc-gen-go; run: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" && exit 1)
	@[ -x "$(PROTOC_GEN_GO_GRPC)" ] || (echo "Missing protoc-gen-go-grpc; run: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest" && exit 1)
	protoc -I $(PROTO_DIR) \
	  --go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
	  --go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative \
	  $(PROTO_SRC)

build:
	go build ./...

tidy:
	go mod tidy

# Start each process on a fixed localhost port (placeholders; real wiring later)
run-local:
	@echo "Start each in its own terminal:"
	@echo "  go run ./cmd/controller --addr=:7001"
	@echo "  go run ./cmd/sink       --addr=:7105"
	@echo "  go run ./cmd/wcount     --addr=:7104 --downstream=127.0.0.1:7105"
	@echo "  go run ./cmd/tokenize   --addr=:7103 --downstream=127.0.0.1:7104"
	@echo "  go run ./cmd/filter     --addr=:7102 --downstream=127.0.0.1:7103"
	@echo "  go run ./cmd/source     --addr=:7101 --downstream=127.0.0.1:7102"

clean:
	rm -rf ./bin

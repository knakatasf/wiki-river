# ----- config
PROTO_DIR := internal/proto
PROTO_SRC := $(wildcard $(PROTO_DIR)/*/*.proto)

# ----- tools
GOBIN := $(shell go env GOPATH)/bin
PROTOC_GEN_GO := $(GOBIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(GOBIN)/protoc-gen-go-grpc

# ----- targets
.PHONY: proto

proto: $(PROTO_SRC)
	@[ -x "$(PROTOC_GEN_GO)" ] || (echo "Missing protoc-gen-go; run: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" && exit 1)
	@[ -x "$(PROTOC_GEN_GO_GRPC)" ] || (echo "Missing protoc-gen-go-grpc; run: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest" && exit 1)
	protoc -I $(PROTO_DIR) \
	  --go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
	  --go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative \
	  $(PROTO_SRC)

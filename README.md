# init go module
go mod init github.com/knakata/rivulet

brew install protobuf

# Go plugins for protobuf + gRPC
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# make sure $GOBIN is on PATH so 'protoc' can find the plugins
export PATH="$(go env GOPATH)/bin:$PATH"

# Add runtime deps you’ll definitely use
go get google.golang.org/grpc@latest
go get google.golang.org/protobuf@latest

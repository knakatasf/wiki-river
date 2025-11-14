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

# Make sure your codegen tools are installed and on PATH:
# go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
# go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

make proto


# how to run
controller:
go run ./cmd/controller --addr=:7001 \
--filter-addr=127.0.0.1:7102 \
--tokenize-addr=127.0.0.1:7103 \
--wcount-addr=127.0.0.1:7104 \
--sink-addr=127.0.0.1:7105

source:
ROLE=SOURCE go run ./cmd/source \
--controller=127.0.0.1:7001 \
--sse-url=https://stream.wikimedia.org/v2/stream/recentchange

filter:
ROLE=FILTER go run ./cmd/filter --addr=:7102 --controller=127.0.0.1:7001

tokenize:
ROLE=TOKENIZE go run ./cmd/tokenize --addr=:7103 --controller=127.0.0.1:7001

wcount:
ROLE=WCOUNT go run ./cmd/wcount   --addr=:7104 --controller=127.0.0.1:7001

sink:
ROLE=SINK go run ./cmd/sink --addr=:7105 --controller=127.0.0.1:7001 --db=results.db

sqlite:
sqlite3 results.db
DELETE FROM word_counts;
VACUUM;
.exit

SELECT window_id, wiki, word, count
FROM word_counts
ORDER BY count DESC
LIMIT 20;

Controller-1:
go run ./cmd/controller \
--addr=:7001 \
--node-id=ctrl-1 \
--raft-bind=127.0.0.1:9001 \
--raft-dir=./data/ctrl-1 \
--raft-bootstrap=true \
--http-addr=:7100

Controller-2:
go run ./cmd/controller \
--addr=:7002 \
--node-id=ctrl-2 \
--raft-bind=127.0.0.1:9002 \
--raft-dir=./data/ctrl-2 \
--raft-bootstrap=false \
--http-addr=:7106 \
--join=http://127.0.0.1:7100

Controller-3:
go run ./cmd/controller \
--addr=:7003 \
--node-id=ctrl-3 \
--raft-bind=127.0.0.1:9003 \
--raft-dir=./data/ctrl-3 \
--raft-bootstrap=false \
--http-addr=:7107 \
--join=http://127.0.0.1:7100


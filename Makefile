run-gc:
	GODEBUG=gctrace=1 go run .

test-cover:
	make clean
	go test ./... -race -coverpkg=./... -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html

fuzz-test:
	go test -run XXX -fuzz=FuzzTestZSet -fuzztime 30s ./internal/zset
	go test -run XXX -fuzz=FuzzTestMap -fuzztime 30s ./internal/hash
	go test -run XXX -fuzz=FuzzTestList -fuzztime 30s ./internal/list

diff-test:
	go test -run TestDifferential ./internal/server -diff.ops=300000 -v

PPROF_HOST ?= localhost:6060

pprof:
	go tool pprof -http=:18081 "http://$(PPROF_HOST)/debug/pprof/profile?seconds=30"

heap:
	go tool pprof http://$(PPROF_HOST)/debug/pprof/heap

clean:
	rm -rf coverage.* *.aof *.rdb appendonlydir

bench:
	go test -run XXX -bench . -benchmem ./internal/...

build:
	CGO_ENABLED=0 \
	go build -o rotom -ldflags "-s -w -X main.buildTime=$(shell date +%y%m%d_%H%M%S%z)"

build-docker:
	docker build --build-arg BUILD_TIME=$(shell date +%y%m%d_%H%M%S%z) -t rotom .

run-redis:
	docker run --rm -p 6379:6379 --name redis-test redis
run-gc:
	GODEBUG=gctrace=1 go run .

test-cover:
	make clean
	go test ./... -race -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html
	make clean

fuzz-test:
	go test -fuzz=FuzzRESPReader

pprof:
	go tool pprof -http=:18081 "http://192.168.10.139:6060/debug/pprof/profile?seconds=30"

heap:
	go tool pprof http://192.168.1.6:6060/debug/pprof/heap

clean:
	rm -f coverage.* *.aof *.rdb

bench:
	go test -bench . -benchmem

build:
	CGO_ENABLED=0 \
	go build -o rotom -ldflags "-s -w -X main.buildTime=$(shell date +%y%m%d_%H%M%S%z)"

build-docker:
	docker build --build-arg BUILD_TIME=$(shell date +%y%m%d_%H%M%S%z) -t rotom .
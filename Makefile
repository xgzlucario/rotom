run:
	go run .

run-gc:
	GODEBUG=gctrace=1 go run .

test-cover:
	go test ./... -race -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html
	rm coverage.txt
	rm *.aof

fuzz-test:
	go test -fuzz=FuzzRESPReader

pprof:
	go tool pprof -http=:18081 "http://192.168.1.6:6060/debug/pprof/profile?seconds=30"

heap:
	go tool pprof http://192.168.1.6:6060/debug/pprof/heap

bench:
	go test -bench . -benchmem

build:
	go build -o rotom -ldflags "-s -w -X main.buildTime=$(shell date +%y%m%d_%H%M%S%z)"

build-docker:
	docker build --build-arg BUILD_TIME=$(shell date +%y%m%d_%H%M%S%z) -t rotom .
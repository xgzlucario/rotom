run:
	go run .

run-gc:
	GODEBUG=gctrace=1 go run .

test-cover:
	go test -race -v -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html

pprof:
	go tool pprof -http=:18081 "http://localhost:6060/debug/pprof/profile?seconds=30"

heap:
	go tool pprof http://localhost:6060/debug/pprof/heap
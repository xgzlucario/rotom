run:
	rm -f rotom.db
	go run example/*.go

run-db:
	go run example/*.go

run-gc:
	rm -f rotom.db
	GODEBUG=gctrace=1 go run main.go

test-cover:
	go test -race \
	-coverpkg=./... \
	-coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html
	make clean

pprof:
	go tool pprof -http=:18081 "http://localhost:6060/debug/pprof/profile?seconds=60"

heap:
	go tool pprof http://localhost:6060/debug/pprof/heap

run-bench:
	go run benchmark/*.go
	make clean

clean:
	rm -f coverage.txt
	rm -r tmp-*
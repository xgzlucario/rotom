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

	rm coverage.txt
	rm *.db

pprof:
	go tool pprof -http=:18081 "http://localhost:6060/debug/pprof/profile?seconds=60"

heap:
	go tool pprof http://localhost:6060/debug/pprof/heap

run-bench:
	rm -rf *.db benchmark/*.db
	go run benchmark/*.go
	rm -rf *.db benchmark/*.db
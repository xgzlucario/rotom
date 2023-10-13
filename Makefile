run:
	rm -f rotom.db
	go run examples/rotom/*.go

run-db:
	go run examples/rotom/*.go

run-client:
	go run examples/client/*.go

run-gc:
	rm -f rotom.db
	GODEBUG=gctrace=1 go run main.go

test-cover:
	go test -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	rm coverage.out
	rm *.db

pprof:
	go tool pprof -http=:18081 "http://localhost:6060/debug/pprof/profile?seconds=60"

heap:
	go tool pprof http://localhost:6060/debug/pprof/heap
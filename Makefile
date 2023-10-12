run:
	rm -f rotom.db
	go build -ldflags="-s -w" -o rotom
	./rotom

rundb:
	go build -ldflags="-s -w" -o rotom
	./rotom

gc-trace-run:
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
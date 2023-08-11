run:
	rm rotom.rdb
	go build -ldflags="-s -w" -o rotom
	./rotom

run2:
	go build -ldflags="-s -w" -o rotom
	./rotom

gc-trace-run:
	rm rotom.rdb
	GODEBUG=gctrace=1 go run main.go

pprof:
	go tool pprof -http=:18081 "http://localhost:6060/debug/pprof/profile?seconds=60"
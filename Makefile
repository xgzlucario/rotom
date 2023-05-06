build-run:
	go build -ldflags="-s -w" -o rotom
	rm -rf db/
	./rotom

pprof:
	go tool pprof -http=:8081 "http://localhost:6060/debug/pprof/profile?seconds=60"
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
	go test -coverprofile=c.out
	go tool cover -html=c.out -o coverage.html
	rm c.out
	rm *.db

test-cover-structx:
	cd structx && bash -c "go test -coverprofile=c.out && go tool cover -html=c.out -o coverage.html && rm c.out"

pprof:
	go tool pprof -http=:18081 "http://localhost:6060/debug/pprof/profile?seconds=60"

heap:
	go tool pprof http://localhost:6060/debug/pprof/heap
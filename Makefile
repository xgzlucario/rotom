run:
	go run .

run-gc:
	GODEBUG=gctrace=1 go run .

test-cover:
	go test -race -v -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html
	rm coverage.txt

pprof:
	go tool pprof -http=:18081 "http://192.168.1.6:6060/debug/pprof/profile?seconds=30"

heap:
	go tool pprof http://localhost:6060/debug/pprof/heap

build-docker:
	docker build -t rotom .

# rsync -av --exclude='.git' rotom/ 2:~/xgz/rotom
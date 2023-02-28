build:
	go build -ldflags="-s -w" -o rotom

upx-build:
	go build -ldflags="-s -w" -o rotom && upx -9 rotom

test:
	rm -rf db && go run *.go
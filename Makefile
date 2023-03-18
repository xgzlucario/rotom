build:
	go build -ldflags="-s -w" -o rotom

upx-build:
	go build -ldflags="-s -w" -o rotom && upx -9 rotom

testa:
	rm -rf db && go run *.go
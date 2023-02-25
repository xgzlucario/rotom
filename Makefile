build:
	go build -ldflags="-s -w"

upx-build:
	go build -ldflags="-s -w" && upx -9 rotom

test:
	go run *.go
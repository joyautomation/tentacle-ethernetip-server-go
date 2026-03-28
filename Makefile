build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o tentacle-ethernetip-server-go .

run: build
	./tentacle-ethernetip-server-go

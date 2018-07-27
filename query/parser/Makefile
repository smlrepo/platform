all: flux.go

flux.go: flux.peg parser.go parser_debug.go | ../../vendor
	PATH=../../bin/${GOOS}:${PATH} $(GO_GENERATE) -x ./...

clean:
	rm -f flux.go

.PHONY: all clean

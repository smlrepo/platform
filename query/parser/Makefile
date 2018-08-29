all: flux.go

flux.go: flux.peg parser.go parser_debug.go
	$(GO_GENERATE) -x ./...

clean:
	rm -f flux.go

.PHONY: all clean

all: promql.go

promql.go: promql.peg gen.go
	go generate -x ./...

clean:
	rm -f promql.go

.PHONY: all clean

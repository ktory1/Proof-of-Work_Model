.PHONY: all worker coordinator client tracing-server config-gen clean

all: worker coordinator client tracing-server

worker:
	go build -o worker cmd/worker/main.go

coordinator:
	go build -o coordinator cmd/coordinator/main.go

client:
	go build -o client cmd/client/main.go

tracing-server:
	go build -o tracing-server cmd/tracing-server/main.go

config-gen:
	go build -o config-gen cmd/config-gen/main.go

clean:
	rm worker coordinator client tracing-server *".log" *"-Log.txt" 2> /dev/null || true
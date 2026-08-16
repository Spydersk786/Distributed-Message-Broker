.PHONY: build run clean test

build:
	@echo "Building broker binary..."
	go build -o bin/broker cmd/server/main.go

run:
	@echo "Starting 3-node cluster..."
	docker-compose up --build

clean:
	@echo "Cleaning up data volumes and binaries..."
	docker-compose down -v
	rm -rf data/ bin/

test:
	@echo "Running integration tests..."
	go run pkg/client/main.go
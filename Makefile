all: validate

validate:
	@go build ./...
	@go vet ./cmd/...
	@go vet ./lib/...
	@go tool vet -shadow cmd/twister/
	@go tool vet -shadow lib/twister/
	@golint ./cmd/...
	@golint ./lib/...
	@ineffassign cmd/twister/
	@ineffassign lib/twister/

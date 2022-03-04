.PHONY: coverage

prepare: tidy fmt vet test

tidy:
	go mod tidy

fmt:
	go fmt ./...

lint:
	revive -config revive.toml -formatter friendly ./...

vet:
	go vet ./...

test:
	go test ./... -count=1 -cover

benchmark:
	go test ./... -count=1 -bench=. -benchmem

coverage:
	go test ./... -count=1 -coverprofile=./coverage/coverage.out && go tool cover -func=coverage/coverage.out

godoc:
	open http://localhost:7777/pkg/github.com/joycastle/bingo/ && godoc -http=:7777

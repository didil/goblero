test:
	go test ./pkg/...
test-cover:
	go test -coverprofile cover.out ./pkg/...
	go tool cover -html=cover.out -o cover.html
	open cover.html
test-ci:
	go test -race -coverprofile=coverage.txt -covermode=atomic ./pkg/...
deps:
	go get -u github.com/dgraph-io/badger
	go get -u github.com/stretchr/testify/assert
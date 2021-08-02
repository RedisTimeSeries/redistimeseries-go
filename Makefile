# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GODOC=GO111MODULE=on godoc
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt

.PHONY: all test coverage
all: test coverage

checkfmt:
	@echo 'Checking gofmt';\
 	bash -c "diff -u <(echo -n) <(gofmt -d .)";\
	EXIT_CODE=$$?;\
	if [ "$$EXIT_CODE"  -ne 0 ]; then \
		echo '$@: Go files must be formatted with gofmt'; \
	fi && \
	exit $$EXIT_CODE

lint:
	@golangci-lint run

doc:
	$(GODOC)

get:
	$(GOGET) -t -v ./...

fmt:
	$(GOFMT) ./...

test: get fmt
	$(GOTEST) -count=1 -race -covermode=atomic ./...

coverage: get test
	$(GOTEST) -race -coverprofile=coverage.txt -covermode=atomic .


HASH=$(shell git describe --tags)
BUILDDATE=$(shell date '+%Y/%m/%d %H:%M:%S %Z')
GOVERSION=$(shell go version)

BUILD_FLAG=-ldflags "-X 'main.version=$(HASH)' -X 'main.date=$(BUILDDATE)' -X 'main.gover=$(GOVERSION)'"

build:
	go build $(BUILD_FLAG)

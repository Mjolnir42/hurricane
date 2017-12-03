# vim: set ft=make ffs=unix fenc=utf8:
# vim: set noet ts=4 sw=4 tw=72 list:
#
all: freebsd linux

validate:
	@go build ./...
	@go vet ./cmd/...
	@go vet ./internal/...
	@go tool vet -shadow cmd/hurricane/
	@go tool vet -shadow internal/cpu/
	@go tool vet -shadow internal/ctx/
	@go tool vet -shadow internal/disk/
	@go tool vet -shadow internal/hurricane/
	@go tool vet -shadow internal/intf/
	@go tool vet -shadow internal/mem/
	@golint ./cmd/...
	@golint ./internal/...
	@ineffassign cmd/hurricane/
	@ineffassign internal/cpu/
	@ineffassign internal/ctx/
	@ineffassign internal/disk/
	@ineffassign internal/hurricane/
	@ineffassign internal/intf/
	@ineffassign internal/mem/

freebsd: validate
	@env GOOS=freebsd GOARCH=amd64 go install -ldflags "-X main.buildtime=`date -u +%Y-%m-%dT%H:%M:%S%z` -X main.githash=`git rev-parse HEAD` -X main.shorthash=`git rev-parse --short HEAD` -X main.builddate=`date -u +%Y%m%d`" ./...

linux: validate
	@env GOOS=linux GOARCH=amd64 go install -ldflags "-X main.buildtime=`date -u +%Y-%m-%dT%H:%M:%S%z` -X main.githash=`git rev-parse HEAD` -X main.shorthash=`git rev-parse --short HEAD` -X main.builddate=`date -u +%Y%m%d`" ./...

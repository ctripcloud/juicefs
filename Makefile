export GO111MODULE=on

all: juicefs

REVISION := $(shell git rev-parse --short HEAD 2>/dev/null)
REVISIONDATE := $(shell git log -1 --pretty=format:'%cd' --date short 2>/dev/null)
TARGETUSER ?= 
PKG := github.com/juicedata/juicefs/pkg/version
LDFLAGS = -s -w
ifneq ($(strip $(REVISION)),) # Use git clone
	LDFLAGS += -X $(PKG).revision=$(REVISION) \
		   -X $(PKG).revisionDate=$(REVISIONDATE) \
			 -X $(PKG).targetUser=$(TARGETUSER)
endif

SHELL = /bin/sh

ifdef STATIC
	LDFLAGS += -linkmode external -extldflags '-static'
	CC = /usr/bin/musl-gcc
	export CC
endif

juicefs: Makefile cmd/*.go pkg/*/*.go go.*
	go version
	go generate
	go build -mod=vendor -ldflags="$(LDFLAGS)"  -o juicefs .

juicefs.cover: Makefile cmd/*.go pkg/*/*.go go.*
	go version
	go generate
	go build -ldflags="$(LDFLAGS)"  -cover -o juicefs .

juicefs.lite: Makefile cmd/*.go pkg/*/*.go
	go generate
	go build -tags nogateway,nowebdav,nocos,nobos,nohdfs,noibmcos,noobs,nooss,noqingstor,noscs,nosftp,noswift,noupyun,noazure,nogs,noufile,nob2,nonfs,nodragonfly,nosqlite,nomysql,nopg,notikv,nobadger,noetcd \
		-ldflags="$(LDFLAGS)" -o juicefs.lite .

juicefs.ceph: Makefile cmd/*.go pkg/*/*.go
	go generate
	go build -tags ceph -ldflags="$(LDFLAGS)"  -o juicefs.ceph .

juicefs.fdb: Makefile cmd/*.go pkg/*/*.go
	go generate
	go build -tags fdb -ldflags="$(LDFLAGS)"  -o juicefs.fdb .

juicefs.gluster: Makefile cmd/*.go pkg/*/*.go
	go generate
	go build -tags gluster -ldflags="$(LDFLAGS)"  -o juicefs.gluster .

juicefs.all: Makefile cmd/*.go pkg/*/*.go
	go generate
	go build -tags ceph,fdb,gluster -ldflags="$(LDFLAGS)"  -o juicefs.all .

# This is the script for compiling the Linux version on the MacOS platform.
# Please execute the `brew install FiloSottile/musl-cross/musl-cross` command before using it.
juicefs.linux:
	go generate
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 CC=x86_64-linux-musl-gcc CGO_LDFLAGS="-static" go build -mod=vendor -ldflags="$(LDFLAGS)"  -o juicefs .

/usr/local/include/winfsp:
	sudo mkdir -p /usr/local/include/winfsp
	sudo cp hack/winfsp_headers/* /usr/local/include/winfsp

# This is the script for compiling the Windows version on the MacOS platform.
# Please execute the `brew install mingw-w64` command before using it.
juicefs.exe: /usr/local/include/winfsp cmd/*.go pkg/*/*.go
	GOOS=windows CGO_ENABLED=1 CC=x86_64-w64-mingw32-gcc \
	     go build -ldflags="$(LDFLAGS)" -buildmode exe -o juicefs.exe .

.PHONY: snapshot release test
snapshot:
	docker run --rm --privileged \
		-e REVISIONDATE=$(REVISIONDATE) \
		-e PRIVATE_KEY=${PRIVATE_KEY} \
		-v ~/go/pkg/mod:/go/pkg/mod \
		-v `pwd`:/go/src/github.com/juicedata/juicefs \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-w /go/src/github.com/juicedata/juicefs \
		juicedata/golang-cross:latest release --snapshot --rm-dist --skip-publish

release:
	docker run --rm --privileged \
		-e REVISIONDATE=$(REVISIONDATE) \
		-e PRIVATE_KEY=${PRIVATE_KEY} \
		--env-file .release-env \
		-v ~/go/pkg/mod:/go/pkg/mod \
		-v `pwd`:/go/src/github.com/juicedata/juicefs \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-w /go/src/github.com/juicedata/juicefs \
		juicedata/golang-cross:latest release --rm-dist

test.meta.core:
	SKIP_NON_CORE=true go test -v -cover -count=1  -failfast -timeout=12m ./pkg/meta/... -coverprofile=cov.out

test.meta.non-core:
	go test -v -cover -run='TestRedisCluster|TestPostgreSQLClient|TestLoadDumpSlow|TestEtcdClient|TestKeyDB' -count=1  -failfast -timeout=12m ./pkg/meta/... -coverprofile=cov.out

test.pkg:
	go test -tags gluster -v -cover -count=1  -failfast -timeout=12m $$(go list ./pkg/... | grep -v /meta) -coverprofile=cov.out

test.cmd:
	sudo JFS_GC_SKIPPEDTIME=1 MINIO_ACCESS_KEY=testUser MINIO_SECRET_KEY=testUserPassword GOMAXPROCS=8 go test -v -count=1 -failfast -cover -timeout=8m ./cmd/... -coverprofile=cov.out -coverpkg=./pkg/...,./cmd/...

test.fdb:
	go test -v -cover -count=1  -failfast -timeout=4m ./pkg/meta/ -tags fdb -run=TestFdb -coverprofile=cov.out

# Protocol Buffers
PROTO_DIR = proto
PROTO_OUT_DIR = pkg/proxy/v1

.PHONY: proto
proto: ## Generate protobuf code
	@echo "Generating protobuf code..."
	@mkdir -p $(PROTO_OUT_DIR)
	@protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(PROTO_OUT_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT_DIR) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto
	@echo "Protobuf code generation completed"

.PHONY: proto-clean
proto-clean: ## Clean generated protobuf code
	@echo "Cleaning generated protobuf code..."
	@rm -rf $(PROTO_OUT_DIR)/*.pb.go
	@echo "Protobuf code cleanup completed"

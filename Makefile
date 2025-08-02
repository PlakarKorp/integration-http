GO=go

all: build

build:
	${GO} build -v -o httpStorage ./plugin/storage

clean:
	rm -f httpStorage http-*.ptar

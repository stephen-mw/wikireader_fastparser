.PHONY: build
build:
	for GOOS in darwin linux windows; do go build -v -o build/parse_xml_$$GOOS; done

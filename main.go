package main

import (
	"flag"
	"path"
	"path/filepath"

	"github.com/stephen-mw/wikireader_fastparse/xml"
)

func main() {
	in := flag.String("in", "", "The input file to process.")
	out := flag.String("out", "", "The output file.")
	workers := flag.Int("workers", 1, "How many worker tasks.")
	flag.Parse()

	// We make some assumptions about the directory structure. Mostly that you have your dumps in the build/ subdirectory of the repo
	dir := filepath.Dir(*in)
	parseXMLScript := path.Join(dir, "../scripts", "parse_xml")

	w := xml.NewWorker(*in, *out, parseXMLScript, *workers)
	w.Start()
}

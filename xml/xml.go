package xml

import (
	"bytes"
	"encoding/xml"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

var parseXMLScript string

// Page is a wikimedia xml page
type Page struct {
	XMLName  xml.Name `xml:"page"`
	Text     string   `xml:",chardata"`
	Title    string   `xml:"title"`
	Ns       string   `xml:"ns"`
	ID       string   `xml:"id"`
	Redirect struct {
		Text  string `xml:",chardata"`
		Title string `xml:"title,attr"`
	} `xml:"redirect"`
	Revision struct {
		Chardata    string `xml:",chardata"`
		ID          string `xml:"id"`
		Parentid    string `xml:"parentid"`
		Timestamp   string `xml:"timestamp"`
		Contributor struct {
			Text     string `xml:",chardata"`
			Username string `xml:"username"`
			ID       string `xml:"id"`
		} `xml:"contributor"`
		Comment string `xml:"comment"`
		Model   string `xml:"model"`
		Format  string `xml:"format"`
		Text    struct {
			Text  string `xml:",innerxml"`
			Bytes string `xml:"bytes,attr"`
			Space string `xml:"space,attr"`
		} `xml:"text"`
		Sha1 string `xml:"sha1"`
	} `xml:"revision"`
}

// seen is used for tracking a list of titles we've seen
var seen = make([]string, 0)

// We don't preserve the XML head from the file, just a dummy one.
var head = []byte(`
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.10/ http://www.mediawiki.org/xml/export-0.10.xsd" version="0.10" xml:lang="en">
    <sitename>Wikipedia</sitename>
    <dbname>enwiki</dbname>
    <base>https://en.wikipedia.org/wiki/Main_Page</base>
    <generator>MediaWiki 1.35.0-wmf.31</generator>
    <case>first-letter</case>
    <namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
      <namespace key="-1" case="first-letter">Special</namespace>
      <namespace key="0" case="first-letter" />
      <namespace key="1" case="first-letter">Talk</namespace>
      <namespace key="2" case="first-letter">User</namespace>
      <namespace key="3" case="first-letter">User talk</namespace>
      <namespace key="4" case="first-letter">Wikipedia</namespace>
      <namespace key="5" case="first-letter">Wikipedia talk</namespace>
      <namespace key="6" case="first-letter">File</namespace>
      <namespace key="7" case="first-letter">File talk</namespace>
      <namespace key="8" case="first-letter">MediaWiki</namespace>
      <namespace key="9" case="first-letter">MediaWiki talk</namespace>
      <namespace key="10" case="first-letter">Template</namespace>
      <namespace key="11" case="first-letter">Template talk</namespace>
      <namespace key="12" case="first-letter">Help</namespace>
      <namespace key="13" case="first-letter">Help talk</namespace>
      <namespace key="14" case="first-letter">Category</namespace>
      <namespace key="15" case="first-letter">Category talk</namespace>
      <namespace key="100" case="first-letter">Portal</namespace>
      <namespace key="101" case="first-letter">Portal talk</namespace>
      <namespace key="108" case="first-letter">Book</namespace>
      <namespace key="109" case="first-letter">Book talk</namespace>
      <namespace key="118" case="first-letter">Draft</namespace>
      <namespace key="119" case="first-letter">Draft talk</namespace>
      <namespace key="446" case="first-letter">Education Program</namespace>
      <namespace key="447" case="first-letter">Education Program talk</namespace>
      <namespace key="710" case="first-letter">TimedText</namespace>
      <namespace key="711" case="first-letter">TimedText talk</namespace>
      <namespace key="828" case="first-letter">Module</namespace>
      <namespace key="829" case="first-letter">Module talk</namespace>
      <namespace key="2300" case="first-letter">Gadget</namespace>
      <namespace key="2301" case="first-letter">Gadget talk</namespace>
      <namespace key="2302" case="case-sensitive">Gadget definition</namespace>
      <namespace key="2303" case="case-sensitive">Gadget definition talk</namespace>
    </namespaces>
  </siteinfo>
 `)

// Worker is a single XML parser worker.
type Worker struct {
	InPage      chan *Page
	OutText     chan []byte
	OutputFile  string
	InputFile   string
	ParseScript string
	workerCount int
	wg          *sync.WaitGroup
}

// NewWorker returns a new worker
func NewWorker(inputFile, outputFile, parseScript string, workerCount int) *Worker {
	return &Worker{
		InPage:      make(chan *Page, 0),
		OutText:     make(chan []byte, 0),
		OutputFile:  outputFile,
		InputFile:   inputFile,
		ParseScript: parseScript,
		workerCount: workerCount,
		wg:          &sync.WaitGroup{},
	}
}

// Start the main processing.
func (w *Worker) Start() {
	for i := 1; i <= w.workerCount; i++ {
		log.Println("starting worker:", i)
		go w.startWorker()
	}

	go w.startWriter()
	w.startReader()

	// Let the workers finish, then exit
	w.wg.Wait()
	close(w.OutText)
}

// read will iterate through the XML file
func (w *Worker) startReader() {
	dump, err := os.Open(w.InputFile)
	if err != nil {
		panic(err)
	}

	decoder := xml.NewDecoder(dump)

	for {
		t, _ := decoder.Token()
		if t == nil {
			break
		}

		// Inspect the type of the token just read.
		switch se := t.(type) {
		case xml.StartElement:
			if se.Name.Local == "page" {
				var p Page
				decoder.DecodeElement(&p, &se)

				found := find(seen, p.Title)
				if found {
					log.Printf("Duplicate title: %s. Skipping...", p.Title)
					continue
				}

				w.InPage <- &p
			}
		}
	}

	// Close the channels associated with reading/writing
	close(w.InPage)
	log.Println("Reader done")
}

// startWriter will start the new xml writer
func (w *Worker) startWriter() {
	f, err := os.Create(w.OutputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Write the header
	_, err = f.Write(head)
	if err != nil {
		panic(err)
	}

	// Write all of the incoming pages, when the channel closes will exit
	for text := range w.OutText {
		// Remove HTML carriage return added as a product of xml marshing
		text := strings.Replace(string(text), "&#xA;", "", -1)

		// Write a newline
		_, err := f.Write([]byte("\n"))
		if err != nil {
			panic(err)
		}

		// Write the article body
		_, err = f.Write([]byte(text))
		if err != nil {
			panic(err)
		}
	}

	// Lastly, close up the file with the final </page> tag
	_, err = f.Write([]byte(`</page>`))
	if err != nil {
		panic(err)
	}

	log.Println("Writer done")
}

// find is a helper function for searching a slice of strings
func find(slice []string, val string) bool {
	for _, p := range slice {
		if p == val {
			return true
		}
	}
	return false
}

// startWorker will start an individual XML worker
func (w *Worker) startWorker() {
	w.wg.Add(1)
	defer w.wg.Done()

	for p := range w.InPage {
		log.Println("processing title: ", p.Title)

		// Skip redirect titles, which have no text that needs parsing
		if strings.HasPrefix(p.Revision.Text.Text, "#REDIRECT") {
			output, err := xml.Marshal(p)
			if err != nil {
				panic(err)
			}
			w.OutText <- output
			continue
		}

		// We will temporarily swap the URL link symbols so we don't parse that
		p.Revision.Text.Text = strings.ReplaceAll(p.Revision.Text.Text, "[[", `<SPEC_START>`)
		p.Revision.Text.Text = strings.ReplaceAll(p.Revision.Text.Text, `]]`, `<SPEC_END>`)

		cmd := exec.Command(w.ParseScript)

		var b bytes.Buffer
		b.Write([]byte(p.Revision.Text.Text))

		cmd.Stdin = &b

		clean, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("error parsing title %s. Skipping", p.Title)
			continue
		}

		// Reverse the url text changes
		new := strings.ReplaceAll(string(clean), `<SPEC_START>`, `[[`)
		new = strings.ReplaceAll(new, `<SPEC_END>`, `]]`)
		p.Revision.Text.Text = new

		output, err := xml.MarshalIndent(p, "  ", "    ")
		if err != nil {
			panic(err)
		}
		w.OutText <- output
	}

	log.Println("exiting xml worker")
}

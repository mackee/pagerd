package main

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"log"
	"os"
	"strings"
)

const (
	XMLPATH  = "minimum.xml"
	JSONPATH = "minimum.json"
)

type Sublink struct {
	Linktype string `xml:"linktype, attr"`
	Anchor   string `xml:"anchor"`
	Link     string `xml:"link"`
}

type Doc struct {
	XMLName  xml.Name  `xml:"doc"`
	Title    string    `xml:"title"`
	Url      string    `xml:"url"`
	Abstract string    `xml:"abstract"`
	Links    []Sublink `xml:"links>sublink"`
}

type JsonRow struct {
	Title    string `json:"title"`
	Url      string `json:"url"`
	Abstract string `json:"abstract"`
}

func main() {
	file, err := os.Open(XMLPATH)
	if err != nil {
		log.Fatal(err)
	}

	jsonfile, err := os.Create(JSONPATH)
	if err != nil {
		log.Fatal(err)
	}

	dec := xml.NewDecoder(file)
	enc := json.NewEncoder(jsonfile)
	replacer := strings.NewReplacer("Wikipedia: ", "")
	i := 0
	for {
		var doc Doc
		if err := dec.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			log.Printf("Parse Error: %s", err)
		}
		replacedTitle := replacer.Replace(doc.Title)
		row := &JsonRow{
			Title:    replacedTitle,
			Url:      doc.Url,
			Abstract: doc.Abstract,
		}
		if err := enc.Encode(row); err != nil {
			log.Printf("Write Error: %s", err)
		}
		i++
		// fmt.Printf("\r% 10d", i)
	}
}

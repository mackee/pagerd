package main

import (
	"encoding/json"
	"fmt"
	"github.com/datastream/btree"
	"io"
	"log"
	"os"
)

type WikiRow struct {
	Title    string `json:"title"`
	Url      string `json:"url"`
	Abstract string `json:"abstract"`
}

const (
	JSONPATH = "../minimum.json"
	DBPATH   = "./pagerdb.bdb"
)

func main() {
	file, err := os.Open(JSONPATH)
	defer file.Close()
	if err != nil {
		log.Fatalf("Open File Error: %s", err)
	}

	db := btree.NewBtree()

	dec := json.NewDecoder(file)
	i := 0
	for {
		var row WikiRow
		if err := dec.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		err := db.Insert([]byte(row.Title), []byte("1"))
		if err != nil {
			log.Fatalf("Insert Error: %s", err)
		}
		i++
		fmt.Printf("\r% 10d", i)
	}

	if err := db.Marshal(DBPATH); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\ndone\n")
}

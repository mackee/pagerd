package main

import (
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
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
	DBPATH   = "../../pagerdb"
)

func main() {
	file, err := os.Open(JSONPATH)
	if err != nil {
		log.Fatalf("Open File Error: %s", err)
	}

	db, err := leveldb.OpenFile(DBPATH, nil)
	defer db.Close()
	if err != nil {
		log.Fatalf("cannot open leveldb file: %s", err)
	}

	dec := json.NewDecoder(file)
	i := 0
	for {
		var row WikiRow
		if err := dec.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Decode Json Error: %s", err)
		}
		title := []byte(row.Title)
		value, err := json.Marshal(row)
		if err != nil {
			log.Fatalf("Encode Json Error: %s", err)
		}
		if err := db.Put(title, value, nil); err != nil {
			log.Fatalf("Put Error: %s", err)
		}

		i++
		fmt.Printf("\r% 10d", i)
	}
	fmt.Printf("\ndone\n")
}

package main

import (
	"encoding/json"
	"fmt"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
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
)

func main() {
	file, err := os.Open(JSONPATH)
	if err != nil {
		log.Fatalf("Open File Error: %s", err)
	}

	db := mysql.New("tcp", "", "127.0.0.1:3306", "root", "", "test")
	if err := db.Connect(); err != nil {
		log.Fatalf("Connect Error: %s", err)
	}

	stmt, err := db.Prepare(`INSERT INTO wikipedia (title, title_full, url, abstract) VALUES(?, ?, ?, ?)`)
	if err != nil {
		log.Fatalf("Prepare Error: %s", err)
	}

	dec := json.NewDecoder(file)
	i := 0
	for {
		var row WikiRow
		if err := dec.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		titleRunes := []rune(row.Title)
		if len(titleRunes) > 191 {
			titleRunes = titleRunes[0:190]
		}
		_, err := stmt.Run(string(titleRunes), row.Title, row.Url, row.Abstract)
		if err != nil {
			log.Fatalf("Query Error: %s", err)
		}
		i++
		fmt.Printf("\r% 10d", i)
	}
	fmt.Printf("\ndone\n")
}

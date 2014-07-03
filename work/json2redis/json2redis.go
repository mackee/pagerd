package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
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
	SORTKEY  = "wikititle"
)

func main() {
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		log.Fatalf("Dial Error: %s", err)
	}

	file, err := os.Open(JSONPATH)
	if err != nil {
		log.Fatalf("Open File Error: %s", err)
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

		err := conn.Send("ZADD", SORTKEY, row.Title, row.Title)
		if err != nil {
			log.Fatalf("Send Error: %s", err)
		}
		i++
		fmt.Printf("\r% 10d", i)
	}
	conn.Flush()
	fmt.Printf("\ndone\n")
}

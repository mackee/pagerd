package main

import (
	"encoding/json"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	"log"
)

type PagerDB struct {
	conn mysql.Conn
	stmt mysql.Stmt
}

type Row struct {
	Title    string `json:"title"`
	Url      string `json:"url"`
	Abstract string `json:"abstract"`
}

func NewPagerDB() *PagerDB {
	conn := mysql.New("tcp", "", "127.0.0.1:3306", "root", "", "test")
	if err := conn.Connect(); err != nil {
		log.Fatalf("Connect Error: %s", err)
	}

	stmt, err := conn.Prepare(`SELECT title_full, url, abstract FROM wikipedia ORDER BY title LIMIT ?, ?`)
	if err != nil {
		log.Fatalf("%s", err)
	}
	pagerDb := &PagerDB{
		conn: conn,
		stmt: stmt,
	}

	return pagerDb
}

func (db *PagerDB) Close() {
	db.conn.Close()
}

func (db *PagerDB) GetByOffset(offset int, limit int) [][][]byte {
	log.Printf("offset %d, limit %d", offset, limit)
	rows, _, err := db.stmt.Exec(offset, limit)
	if err != nil {
		log.Printf("%s", err)
		return make([][][]byte, 0)
	}

	result := make([][][]byte, 0)
	for _, row := range rows {
		title := row.Str(0)
		jsonRow := &Row{
			Title:    title,
			Url:      row.Str(1),
			Abstract: row.Str(2),
		}
		rowBytes, err := json.Marshal(jsonRow)
		if err != nil {
			log.Printf("%s", err)
			return make([][][]byte, 0)
		}
		result = append(result, [][]byte{[]byte(title), rowBytes})
	}

	return result
}

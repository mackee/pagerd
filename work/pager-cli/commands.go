package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/jmhodges/levigo"
	"log"
	"os"
	"strconv"
)

var Commands = []cli.Command{
	commandOffset,
	commandLimitby,
	commandGet,
	commandSet,
}

var commandOffset = cli.Command{
	Name:  "offset",
	Usage: "-d pathto/leveldb <offset> <limit>",
	Description: `
offset:	integer, offset from first of db (required)
limit:	integer, limit by start key (required)
`,
	Action: doOffset,
	Flags: []cli.Flag{
		cli.StringFlag{"dbpath, d", "pagerdb", "pagerdb directory path"},
	},
}

var commandLimitby = cli.Command{
	Name:  "limitby",
	Usage: "-d pathto/leveldb <key> <limit>",
	Description: `
key:	string, start key (required)
limit:	integer, limit by start key (required)
`,
	Action: doLimitby,
	Flags: []cli.Flag{
		cli.StringFlag{"dbpath, d", "pagerdb", "pagerdb directory path"},
	},
}

var commandGet = cli.Command{
	Name:  "get",
	Usage: "-d pathto/leveldb <key>",
	Description: `
key:	string, want key (required)
`,
	Action: doGet,
	Flags: []cli.Flag{
		cli.StringFlag{"dbpath, d", "pagerdb", "pagerdb directory path"},
	},
}

var commandSet = cli.Command{
	Name:  "set",
	Usage: "-d pathto/leveldb <key> <value>",
	Description: `
key:	string, set key (required)
value:	string, set value (required)
`,
	Action: doSet,
	Flags: []cli.Flag{
		cli.StringFlag{"dbpath, d", "pagerdb", "pagerdb directory path"},
	},
}

func debug(v ...interface{}) {
	if os.Getenv("DEBUG") != "" {
		log.Println(v...)
	}
}

func assert(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func doOffset(c *cli.Context) {
	dbpath := c.String("dbpath")
	db := connectDB(dbpath)
	defer db.Close()

	args := c.Args()
	if len(args) < 2 {
		log.Fatalf("Input Error: args is empty")
	}
	offset, err := strconv.Atoi(string(args[0]))
	if err != nil {
		log.Fatalf("Args(offset) Error: %s", err)
	}
	limit, err := strconv.Atoi(string(args[1]))
	if err != nil {
		log.Fatalf("Args(limit) Error: %s", err)
	}
	log.Printf("offset: %d, limit: %d", offset, limit)

	ro := levigo.NewReadOptions()
	iter := db.NewIterator(ro)
	iter.SeekToFirst()
	for i := 0; i < offset; i++ {
		iter.Next()
		if !iter.Valid() {
			log.Fatalf("this offset is too large\n")
		}
	}
	for i := 0; iter.Valid() && i < limit; iter.Next() {
		fmt.Printf("key: %s, value: %s\n", iter.Key(), iter.Value())
		i++
	}
}

func doLimitby(c *cli.Context) {
	dbpath := c.String("dbpath")
	db := connectDB(dbpath)
	defer db.Close()

	args := c.Args()
	if len(args) < 2 {
		log.Fatalf("Input Error: args is empty")
	}
	key := args[0]
	limit, err := strconv.Atoi(string(args[1]))
	if err != nil {
		log.Fatalf("Args Error: %s", err)
	}
	log.Printf("key: %s, limit: %d", key, limit)

	ro := levigo.NewReadOptions()
	iter := db.NewIterator(ro)
	iter.Seek([]byte(key))
	for i := 0; iter.Valid() && i < limit; iter.Next() {
		fmt.Printf("key: %s, value: %s\n", iter.Key(), iter.Value())
		i++
	}
}

func doGet(c *cli.Context) {
	dbpath := c.String("dbpath")
	db := connectDB(dbpath)
	defer db.Close()

	args := c.Args()
	if len(args) == 0 {
		log.Fatalf("Input Error: Key is empty")
	}
	key := args[0]
	log.Printf("key: %s\n", key)

	ro := levigo.NewReadOptions()
	result, err := db.Get(ro, []byte(key))
	if err != nil {
		log.Fatalf("%s", err)
	}

	fmt.Printf("%s\n", result)
}

func doSet(c *cli.Context) {
	dbpath := c.String("dbpath")
	db := connectDB(dbpath)
	defer db.Close()

	args := c.Args()
	if len(args) < 2 {
		log.Fatalf("Input Error: Args is empty")
	}
	key := args[0]
	value := args[1]
	log.Printf("key: %s, value %s\n", key, value)

	wo := levigo.NewWriteOptions()
	err := db.Put(wo, []byte(key), []byte(value))
	if err != nil {
		log.Fatalf("%s", err)
	}
}

func connectDB(dbpath string) *levigo.DB {
	opt := levigo.NewOptions()
	db, err := levigo.Open(dbpath, opt)
	if err != nil {
		log.Fatalf("DB OpenError: %s", err)
	}

	return db
}

package main

import (
	"github.com/jmhodges/levigo"
	"log"
	"strconv"
)

type PagerDB struct {
	leveldb *levigo.DB
	cachedb *levigo.DB
}

const (
	DBPATH      = "pagerdb"
	CACHEDBPATH = "pagerdbcache"
)

func NewPagerDB() *PagerDB {
	opt := levigo.NewOptions()
	opt.SetCreateIfMissing(true)
	db, err := levigo.Open(DBPATH, opt)
	if err != nil {
		log.Fatalf("DB OpenError: %s", err)
	}
	cachedb, err := levigo.Open(CACHEDBPATH, opt)
	if err != nil {
		log.Fatalf("DB OpenError: %s", err)
	}
	pagerDb := &PagerDB{
		leveldb: db,
		cachedb: cachedb,
	}

	return pagerDb
}

func (db *PagerDB) Close() {
	db.leveldb.Close()
}

func (db *PagerDB) GetByOffset(offset int, limit int) [][][]byte {
	log.Printf("offset: %d, limit: %d", offset, limit)

	cachero := levigo.NewReadOptions()
	cacheIter := db.cachedb.NewIterator(cachero)
	cacheIter.SeekToFirst()
	cursor := 0
	lastKey := []byte("")
	for {
		if !cacheIter.Valid() {
			break
		}
		cursorStr := string(cacheIter.Value())
		var err error
		cursor, err = strconv.Atoi(cursorStr)
		lastKey = cacheIter.Key()
		if err != nil {
			log.Println("cache error %s", err)
			return make([][][]byte, 0)
		}
		if cursor >= offset {
			break
		}
		cacheIter.Next()
	}

	ro := levigo.NewReadOptions()
	iter := db.leveldb.NewIterator(ro)
	cacheOffset := 0
	if cursor == offset {
		cacheOffset = offset
		iter.Seek(lastKey)
	} else if cursor < offset {
		if cursor > 0 {
			cacheOffset = cursor
			iter.Seek(lastKey)
		} else {
			iter.SeekToFirst()
		}
	} else {
		cacheIter.Prev()
		if cacheIter.Valid() {
			startKey := cacheIter.Key()
			iter.Seek(startKey)
			if !iter.Valid() {
				log.Println("offset error: minus value")
				return make([][][]byte, 0)
			}

			cacheOffsetStr := string(cacheIter.Value())
			var err error
			cacheOffset, err = strconv.Atoi(cacheOffsetStr)
			if err != nil {
				log.Println("cache error %s", err)
				return make([][][]byte, 0)
			}
		} else {
			iter.SeekToFirst()
		}
	}

	for i := cacheOffset; i < offset; i++ {
		if !iter.Valid() {
			log.Printf("this offset is too large")
			return make([][][]byte, 0)
		}
		iter.Next()
	}
	wo := levigo.NewWriteOptions()
	db.cachedb.Put(wo, iter.Key(), []byte(strconv.Itoa(offset)))

	result := [][][]byte{}
	for i := 0; iter.Valid() && i < limit; iter.Next() {
		kv := [][]byte{iter.Key(), iter.Value()}
		result = append(result, kv)
		i++
	}
	return result
}

package main

import (
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"runtime"
	"strconv"
)

var db *PagerDB

func main() {
	runtime.GOMAX
	db = NewPagerDB()
	defer db.Close()

	r := mux.NewRouter()

	r.HandleFunc("/", RootHandler).Methods("GET")
	r.HandleFunc("/offset/{offset}/limit/{limit}", PagerOffsetHandler).Methods("GET")

	http.Handle("/", r)

	http.ListenAndServe(":8080", nil)
}

func RootHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello, Pager\n"))
	log.Printf("%+v", r)
}

func PagerOffsetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	offset, err := strconv.Atoi(vars["offset"])
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	limit, err := strconv.Atoi(vars["limit"])
	if err != nil {
		w.Write([]byte(err.Error()))
	}

	result := db.GetByOffset(offset, limit)
	w.Write([]byte("["))

	for _, kv := range result {
		value := kv[1]
		w.Write(value)
		w.Write([]byte(",\n"))
	}

	w.Write([]byte("]"))
}

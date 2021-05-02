package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"go.samhza.com/bore/filedb"
)

func main() {
	dbpath := flag.String("db", "bore.db", "path to bore db")
	flag.Parse()
	db, err := filedb.Open(*dbpath)
	if err != nil {
		log.Fatalln(err)
	}
	switch flag.Arg(0) {
	case "put":
		if len(flag.Args()) < 3 {
			os.Exit(1)
		}
		tx, err := db.Begin(true)
		if err != nil {
			log.Fatalln(err)
		}
		defer tx.Rollback()
		fname, err := filepath.Abs(flag.Arg(1))
		if err != nil {
			log.Fatalln(err)
		}
		err = tx.Put(fname, flag.Args()[2:])
		if err != nil {
			log.Fatalln(err)
		}
		tx.Commit()
	case "get":
		if len(flag.Args()) < 2 {
			os.Exit(1)
		}
		tx, err := db.Begin(false)
		if err != nil {
			log.Fatalln(err)
		}
		defer tx.Rollback()
		fname, err := filepath.Abs(flag.Arg(1))
		if err != nil {
			log.Fatalln(err)
		}
		tags := tx.Get(fname)
		if tags == nil {
			log.Fatalln("not present in db")
		}
		for _, tag := range tags {
			fmt.Println(tag)
		}
	case "search":
		tx, err := db.Begin(false)
		if err != nil {
			log.Fatalln(err)
		}
		defer tx.Rollback()
		tags := flag.Args()[1:]
		var incl, excl []string
		for _, arg := range tags {
			if arg[0] == '-' {
				excl = append(excl, arg[1:])
				continue
			}
			incl = append(incl, arg)
		}
		results, err := tx.Search(incl, excl)
		if err != nil {
			log.Fatalln(err)
		}
		for _, ent := range results {
			fmt.Println(ent.Filename)
		}
	case "rename":
		if len(flag.Args()) < 3 {
			os.Exit(1)
		}
		tx, err := db.Begin(false)
		if err != nil {
			log.Fatalln(err)
		}
		tx.Rename(flag.Arg(1), flag.Arg(2))
		defer tx.Rollback()
	case "move":
		if len(flag.Args()) < 3 {
			os.Exit(1)
		}
		tx, err := db.Begin(false)
		if err != nil {
			log.Fatalln(err)
		}
		oname, err := filepath.Abs(flag.Arg(1))
		if err != nil {
			log.Fatalln(err)
		}
		nname, err := filepath.Abs(flag.Arg(2))
		if err != nil {
			log.Fatalln(err)
		}
		tx.Move(oname, nname)
		defer tx.Rollback()
	case "delete":
		if len(flag.Args()) < 2 {
			os.Exit(1)
		}
		tx, err := db.Begin(false)
		if err != nil {
			log.Fatalln(err)
		}
		fname, err := filepath.Abs(flag.Arg(1))
		if err != nil {
			log.Fatalln(err)
		}
		tx.Delete(fname)
		defer tx.Rollback()
	default:
		os.Exit(1)
	}
}

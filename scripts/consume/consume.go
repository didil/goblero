package main

import (
	"fmt"
	"log"

	"github.com/didil/goblero/pkg/blero"
)

func main() {
	bl := blero.New(blero.Opts{DBPath: "db/dev"})
	err := bl.Start()
	if err != nil {
		log.Fatal(err)
	}
	// stop gracefully
	defer bl.Stop()

	/*
		err = db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte("q:standard:pending:1"))
			if err != nil {
				return err
			}

			b, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&j)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			log.Fatal(err)
		}*/

	j, err := bl.DequeueJob()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+v\n", j)
}

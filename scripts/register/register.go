package main

import (
	"log"

	"github.com/didil/goblero/pkg/blero"
)

func main() {
	bl := blero.New(blero.BleroOpts{DBPath: "db/dev"})
	err := bl.Start()
	if err != nil {
		log.Fatal(err)
	}
	// stop gracefully
	defer bl.Stop()

	bl.Enqueue("SendEmail")

	if err != nil {
		log.Fatal(err)
	}
}

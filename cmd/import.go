package main

import (
	"fmt"
	"log"
	"os"

	"github.com/go-errors/errors"
	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/hcasfs"
)

func main() {
	h, err := hcas.CreateHcas("test-hcas")
	if err != nil {
		log.Fatal("failed to initialize hcas: ", err)
	}
	defer h.Close()

	session, err := h.CreateSession()
	defer session.Close()

	name, err := hcasfs.ImportPath(session, os.Args[1])
	if err != nil {
		gerr, ok := err.(*errors.Error)
		if ok {
			log.Fatal(err, gerr.ErrorStack())
		} else {
			log.Fatal(err)
		}
	}
	fmt.Printf("imported path to %s\n", hcas.NameHex(name))
}

package main

import (
	"fmt"
	"log"

	"github.com/msg555/hcas/hcas"
)

func main() {
	h, err := hcas.OpenHcas("test-hcas")
	if err != nil {
		log.Fatal("failed to initialize hcas: ", err)
	}

	complete, err := h.GarbageCollect(0)
	if err != nil {
		log.Fatal("failed to collect garbage: ", err)
	}

	fmt.Printf("GC complete: %t\n", complete)
}

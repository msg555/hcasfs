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
	defer func() {
		err := h.Close()
		if err != nil {
			fmt.Printf("Failure cleaning up hcas: %s\n", err)
		} else {
			fmt.Printf("HCAS cleaned up okay!\n")
		}
	}()

	fmt.Printf("HCAS created: %s!\n", h)

	session, err := h.CreateSession()
	if err != nil {
		log.Fatal("failed to create session: ", err)
	}
	defer func() {
		err := session.Close()
		if err != nil {
			fmt.Printf("Failure cleaning up session: %s\n", err)
		} else {
			fmt.Printf("Session cleaned up okay!\n")
		}
	}()

	name, err := session.CreateObject([]byte("hello hcas!"))
	if err != nil {
		log.Fatal("failed to create object: ", err)
	}
	fmt.Printf("created %s\n", hcas.NameHex(name))

	name2, err := session.CreateObject(
		[]byte("also hi"),
		name,
	)
	if err != nil {
		log.Fatal("failed to create object: ", err)
	}

	err = session.SetLabel("msg-test", name2)
	if err != nil {
		log.Fatal("failed to set label: ", err)
	}

	objName, err := session.GetLabel("msg-test")
	if err != nil {
		fmt.Printf("Failed to get label: %s\n", err)
	} else {
		fmt.Printf("Label value: %s\n", hcas.NameHex(objName))
	}

	fmt.Printf("Session created: %s!\n", session)
}

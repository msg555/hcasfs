package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/msg555/hcas/fusefs"
	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/unix"
)

func getRootObject(hcasRootDir string, hcasRootLabel string) ([]byte, error) {
	h, err := hcas.OpenHcas(hcasRootDir)
	if err != nil {
		return nil, err
	}
	defer h.Close()

	s, err := h.CreateSession()
	if err != nil {
		return nil, err
	}

	return s.GetLabel("image", hcasRootLabel)
}

func main() {
	if len(os.Args) != 4 {
		log.Fatal("Usage: mount mount_point hcas_root object_label")
	}

	mountPoint := os.Args[1]
	hcasRootDir := os.Args[2]
	hcasRootLabel := os.Args[3]
	hcasRootName, err := getRootObject(hcasRootDir, hcasRootLabel)
	if err != nil {
		log.Fatal("failed to find root object name: ", err)
	}

	log.Print("Mounting root object ", hcas.NameHex(hcasRootName))

	hm, err := fusefs.CreateServer(mountPoint, hcasRootDir, hcasRootName)
  if err != nil {
    log.Fatal("failed to create mount", err)
  }

  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs, unix.SIGINT, unix.SIGTERM)
  fmt.Println("signal received: ", <-sigs)

  err = hm.Close()
  if err != nil {
    log.Fatal("Could not unmount:", err)
  }
}

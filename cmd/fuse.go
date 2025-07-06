package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"flag"

	"bazil.org/fuse"

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

	name, err := s.GetLabel("image", hcasRootLabel)
	if err != nil {
		return nil, err
	}
	if name == nil {
		return nil, fmt.Errorf("label not found: %s", hcasRootLabel)
	}
	return []byte(name.Name()), nil
}

func main() {
	flagSet := flag.NewFlagSet("hcas-fuse", flag.ExitOnError)
	flagAllowOther := flagSet.Bool("allow-other", false, "Allow others to see mount")
	flagSet.Parse(os.Args[1:])

	args := flagSet.Args()
	if len(args) != 3 {
		log.Fatal("Usage: mount mount_point hcas_root object_label")
	}

	mountPoint := args[0]
	hcasRootDir := args[1]
	hcasRootLabel := args[2]
	hcasRootName, err := getRootObject(hcasRootDir, hcasRootLabel)
	if err != nil {
		log.Fatal("failed to find root object name: ", err)
	}

	rootName := hcas.NewName(string(hcasRootName))
	log.Print("Mounting root object ", rootName.HexName())

	var options []fuse.MountOption
	if *flagAllowOther {
		options = append(options, fuse.AllowOther())
	}

	hm, err := fusefs.CreateServer(mountPoint, hcasRootDir, hcasRootName, options...)
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

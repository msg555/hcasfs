package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/go-errors/errors"
	"github.com/msg555/hcas/hcas"
	"github.com/msg555/hcas/hcasfs"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: import_tar <tar_file> <label_name>")
	}

	tarFilePath := os.Args[1]
	labelName := os.Args[2]

	// Create or open HCAS instance
	h, err := hcas.CreateHcas("test-hcas")
	if err != nil {
		log.Fatal("failed to initialize hcas: ", err)
	}
	defer h.Close()

	// Create session
	session, err := h.CreateSession()
	if err != nil {
		log.Fatal("failed to create session: ", err)
	}
	defer session.Close()

	// Open tar file
	var reader io.Reader
	if tarFilePath == "-" {
		reader = os.Stdin
	} else {
		file, err := os.Open(tarFilePath)
		if err != nil {
			log.Fatal("failed to open tar file: ", err)
		}
		defer file.Close()

		// Check if it's a gzipped tar file
		if strings.HasSuffix(strings.ToLower(tarFilePath), ".gz") ||
			strings.HasSuffix(strings.ToLower(tarFilePath), ".tgz") {
			gzReader, err := gzip.NewReader(file)
			if err != nil {
				log.Fatal("failed to create gzip reader: ", err)
			}
			defer gzReader.Close()
			reader = gzReader
		} else {
			reader = file
		}
	}

	// Import tar contents
	fmt.Printf("Importing tar archive...\n")
	name, err := hcasfs.ImportTar(session, reader)
	if err != nil {
		gerr, ok := err.(*errors.Error)
		if ok {
			log.Fatal(err, gerr.ErrorStack())
		} else {
			log.Fatal(err)
		}
	}

	fmt.Printf("Imported tar archive to %s\n", name.HexName())

	// Set label
	err = session.SetLabel("image", labelName, name)
	if err != nil {
		log.Fatal("Could not set label: ", err)
	}

	fmt.Printf("Set label '%s' -> %s\n", labelName, name.HexName())
}

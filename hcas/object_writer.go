package hcas

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

const objectWriterBufferSize = 1 << 16

type hcasObjectWriter struct {
	session    *hcasSession
	buffer     []byte
	tempFileId int64
	file       *os.File
	hsh        hash.Hash
	deps       []Name
	name       *Name
}

func createObjectStream(session *hcasSession, deps ...Name) (ObjectWriter, error) {
	return createObjectStreamWithBuffer(
		session,
		make([]byte, 0, objectWriterBufferSize),
		deps...,
	)
}

func createObjectStreamWithBuffer(session *hcasSession, buffer []byte, deps ...Name) (ObjectWriter, error) {
	depsCopy := make([]Name, len(deps))
	copy(depsCopy, deps)
	sort.Slice(depsCopy, func(i, j int) bool {
		return depsCopy[i].Name() < depsCopy[j].Name()
	})

	hsh := sha256.New()

	var numDeps [4]byte
	binary.BigEndian.PutUint32(numDeps[:], uint32(len(deps)))
	hsh.Write(numDeps[:])
	for _, dep := range depsCopy {
		hsh.Write([]byte(dep.Name()))
	}

	hsh.Write(buffer)

	return &hcasObjectWriter{
		session: session,
		buffer:  buffer,
		file:    nil,
		hsh:     hsh,
		deps:    depsCopy,
		name:    nil,
	}, nil
}

func (ow *hcasObjectWriter) makeTempFile() error {
	for {
		file, err := os.CreateTemp(
			filepath.Join(ow.session.hcas.basePath, TempPath),
			"tmp-*",
		)
		if err != nil {
			return err
		}

		err = lockFile(file)
		if err != nil {
			file.Close()
			return err
		}

		_, err = os.Stat(file.Name())
		if err == nil {
			ow.file = file
			break
		}
		file.Close()
		if os.IsNotExist(err) {
			continue
		}
		return err
	}

	// Drain existing buffer into new temp file
	buf := ow.buffer
	ow.buffer = nil
	amount := 0
	for amount < len(buf) {
		n, err := ow.file.Write(buf[amount:])
		if err != nil {
			return err
		}
		amount += n
	}

	return nil
}

func (ow *hcasObjectWriter) Write(p []byte) (int, error) {
	if ow.buffer != nil {
		bufLen := len(ow.buffer)
		if bufLen+len(p) <= cap(ow.buffer) {
			// Happy path, data fits entirely in buffer
			ow.buffer = ow.buffer[:bufLen+len(p)]
			copy(ow.buffer[bufLen:], p)
			ow.hsh.Write(p)
			return len(p), nil
		}

		// Buffer full, create backing temp file
		ow.makeTempFile()
	}

	// If already made a backing temp file just write to that
	n, err := ow.file.Write(p)
	ow.hsh.Write(p[:n])
	return n, err
}

func (ow *hcasObjectWriter) Close() error {
	/* On close we insert the written file into HCAS. The general flow for how a
	   * file is written into HCAS is outlined below:
		 *
		 * 1. Calculate name from content hash and dependencies
		 * 2. Insert new record into temp_objects with calculated name
			 3. Start exclusive transaction
				 a. Delete temp object record
				 b. If extending object lease succeeds
				 	 - Clean up temp file
					 - Commit
				 c. Otherwise
					 - Create new object entry
					 - Setup object deps
					 - Rename temp file into position
					 - Commit
	*/

	name := NewName(string(ow.hsh.Sum(nil)))

	db := ow.session.hcas.db
	result, err := db.Exec(
		"INSERT INTO temp_objects (name) VALUES (?);",
		name.Name(),
	)
	if err != nil {
		return err
	}
	tempObjectId, err := result.LastInsertId()
	if err != nil {
		return err
	}

	// Create the containing data dirs optimistically
	objectDir, objectPath := ow.session.hcas.dataFilePath(name)
	err = os.Mkdir(objectDir, 0o777)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}

	// Ensure file is synced if we created one
	if ow.file != nil {
		err = ow.file.Sync()
		if err != nil {
			return err
		}
	}

	leaseTime := calculateLeaseTime(defaultObjectLease)

	// Start exclusive transaction
	result, err = db.Exec(`
BEGIN IMMEDIATE;

DELETE FROM temp_objects WHERE id=?;

UPDATE objects SET lease_time=MAX(?, lease_time+1) WHERE name = ?;
`, tempObjectId, leaseTime, name.Name())
	if err != nil {
		db.Exec("ROLLBACK")
		return err
	}

	// Handle case where object already exists
	rowCount, err := result.RowsAffected()
	if err != nil {
		db.Exec("ROLLBACK")
		return err
	}
	if rowCount > 0 {
		_, err = db.Exec("COMMIT")
		if err != nil {
			return err
		}

		// Close temp file if we created one
		if ow.file != nil {
			err = ow.file.Close()
			if err != nil {
				return err
			}
		}

		ow.name = &name
		return nil
	}

	// Object doesn't already exists, create it
	result, err = db.Exec(
		"INSERT INTO objects (name, ref_count, lease_time) VALUES (?, 1, ?)",
		name.Name(),
		leaseTime,
	)
	if err != nil {
		db.Exec("ROLLBACK")
		return err
	}

	objectId, err := result.LastInsertId()
	if err != nil {
		db.Exec("ROLLBACK")
		return err
	}

	// Create object dependencies
	for _, dep := range ow.deps {
		row := db.QueryRow("SELECT id FROM objects WHERE name = ?", dep.Name())

		var dep_id int64
		err = row.Scan(&dep_id)
		if err == sql.ErrNoRows {
			db.Exec("ROLLBACK")
			return errors.New("Dependency does not exist")
		} else if err != nil {
			db.Exec("ROLLBACK")
			return err
		}

		_, err = db.Exec(`
INSERT INTO object_deps (parent_id, child_id) VALUES (?, ?);
UPDATE objects SET ref_count = ref_count + 1 WHERE id = ?;
`, objectId, dep_id, dep_id)
		if err != nil {
			db.Exec("ROLLBACK")
			return err
		}
	}

	// Force temp file creation if we haven't done so yet.
	if ow.file == nil {
		err = ow.makeTempFile()
		if err != nil {
			db.Exec("ROLLBACK")
			return err
		}

		err = ow.file.Sync()
		if err != nil {
			db.Exec("ROLLBACK")
			return err
		}
	}

	// TODO: Ought to unlink temp file on exist error
	err = os.Rename(ow.file.Name(), objectPath)
	if err != nil && os.IsNotExist(err) {
		db.Exec("ROLLBACK")
		return err
	}

	// Commit metadata updates
	db.Exec("COMMIT")
	if err != nil {
		return err
	}

	// Close out the file
	err = ow.file.Close()
	if err != nil {
		return err
	}

	fmt.Printf("Object name: %s\n", name.HexName())
	ow.name = &name
	return nil
}

func (ow *hcasObjectWriter) Name() *Name {
	return ow.name
}

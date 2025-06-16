package hcas

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"os"
	"sort"
)

type hcasObjectWriter struct {
	session    *hcasSession
	tempFileId int64
	file       *os.File
	hsh        hash.Hash
	deps       []Name
	name       *Name
}

func createObjectStream(session *hcasSession, deps ...Name) (ObjectWriter, error) {
	// TODO: Is this really the best way to copy this?
	depsCopy := make([]Name, len(deps))
	copy(depsCopy, deps)
	sort.Slice(depsCopy, func(i, j int) bool {
		return depsCopy[i].Name() < depsCopy[j].Name()
	})

	result, err := session.hcas.db.Exec(
		"INSERT INTO temp_files (session_id) VALUES (?);",
		session.sessionId,
	)
	if err != nil {
		return nil, err
	}

	tempFileId, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	tempFilePath := session.hcas.tempFilePath(tempFileId)
	file, err := os.Create(tempFilePath)
	if err != nil {
		return nil, err
	}

	return &hcasObjectWriter{
		session:    session,
		tempFileId: tempFileId,
		file:       file,
		hsh:        sha256.New(),
		deps:       depsCopy,
		name:       nil,
	}, nil
}

func (ow *hcasObjectWriter) Write(p []byte) (int, error) {
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
				 a. Check if object already exists
					 - Attach existing object to session
					 - Clean up temp_files, temp_object entries
					 - Commit
	       b. Otherwise
					 - Create new object entry
					 - Attach new object to session
					 - Clean up temp_files, temp_object entries
					 - Rename temp file into position
					 - Commit
	*/

	// Close out the file and compute the final hash
	err := ow.file.Close()
	if err != nil {
		return err
	}
	name := NewName(string(ow.hsh.Sum(nil)))

	// Insert into temp objects to ensure we don't lose track of file data in case
	// of failure.
	db := ow.session.hcas.db
	_, err = db.Exec(
		"INSERT INTO temp_objects (name) VALUES (?)",
		name.Name(),
	)
	if err != nil {
		return err
	}

	tempFilePath := ow.session.hcas.tempFilePath(ow.tempFileId)

	// Start exclusive transaction
	_, err = db.Exec("BEGIN IMMEDIATE")
	if err != nil {
		return err
	}

	// Check if object already exists
	var existingObjectId int64
	err = db.QueryRow("SELECT id FROM objects WHERE name = ?", name.Name()).Scan(&existingObjectId)
	if err == nil {
		// If it does clear the temp file and we're done
		err = os.Remove(tempFilePath)
		if err != nil {
			db.Exec("ROLLBACK")
			return err
		}

		_, err = db.Exec("DELETE FROM temp_files WHERE id = ?", ow.tempFileId)
		if err != nil {
			db.Exec("ROLLBACK")
			return err
		}

		// Attach existing object to session
		_, err = db.Exec(
			"INSERT INTO session_deps (session_id, object_id) VALUES (?, ?)",
			ow.session.sessionId,
			existingObjectId,
		)
		if err != nil {
			db.Exec("ROLLBACK")
			return err
		}

		_, err = db.Exec("COMMIT")
		if err != nil {
			return err
		}

		ow.name = &name
		return nil
	} else if err != sql.ErrNoRows {
		db.Exec("ROLLBACK")
		return err
	}

	// Add newly created object to metadata
	result, err := db.Exec("INSERT INTO objects (name, ref_count) VALUES (?, 1)", name.Name())
	if err != nil {
		db.Exec("ROLLBACK")
		return err
	}

	objectId, err := result.LastInsertId()
	if err != nil {
		db.Exec("ROLLBACK")
		return err
	}

	// Attach newly created object to session
	_, err = db.Exec(
		"INSERT INTO session_deps (session_id, object_id) VALUES (?, ?)",
		ow.session.sessionId,
		objectId,
	)
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

	// Move object into its target position
	objectDir, objectPath := ow.session.hcas.dataFilePath(name)
	err = os.Mkdir(objectDir, 0o777)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		db.Exec("ROLLBACK")
		return err
	}

	err = os.Rename(tempFilePath, objectPath)
	if err != nil {
		db.Exec("ROLLBACK")
		return err
	}

	// Commit metadata updates
	db.Exec("COMMIT")
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

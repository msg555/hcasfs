package hcas

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
)

type hcasSession struct {
	hcas      *hcasInternal
	sessionId int64
}

func createSession(hcas *hcasInternal) (Session, error) {
	result, err := hcas.db.Exec("INSERT INTO sessions DEFAULT VALUES;")
	if err != nil {
		return nil, err
	}

	sessionId, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &hcasSession{
		hcas:      hcas,
		sessionId: sessionId,
	}, nil
}

func (s *hcasSession) GetLabel(namespace string, label string) ([]byte, error) {
	tx, err := s.hcas.db.Begin()
	if err != nil {
		return nil, err
	}

	row := tx.QueryRow(`
SELECT l.object_id, o.name FROM labels AS l
	JOIN objects AS o ON (l.object_id = o.id)
	WHERE namespace = ? AND label = ?;`, namespace, label)

	var objectId int64
	var name []byte
	err = row.Scan(&objectId, &name)
	if err != nil && err != sql.ErrNoRows {
		tx.Rollback()
		return nil, err
	}

	if name != nil {
		_, err = tx.Exec(`
INSERT INTO session_deps (session_id, object_id) VALUES (?, ?);
UPDATE objects SET ref_count = ref_count + 1 WHERE id = ?;
`, s.sessionId, objectId, objectId)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return name, nil
}

func (s *hcasSession) SetLabel(namespace string, label string, name []byte) error {
	tx, err := s.hcas.db.Begin()
	if err != nil {
		return err
	}

	// Lookup selected object
	var objectId int64
	if name != nil {
		row := tx.QueryRow(
			"SELECT id FROM objects WHERE name = ?",
			name,
		)

		err = row.Scan(&objectId)
		if err == sql.ErrNoRows {
			tx.Rollback()
			return errors.New("Object with name does not exist")
		} else if err != nil {
			tx.Rollback()
			return err
		}
	}

	// Delete existing labeled object if it existed
	if objectId != 0 {
		fmt.Printf("Set label %s to %d\n", label, objectId)
		_, err = tx.Exec(`
UPDATE objects AS o
	SET ref_count = ref_count - 1
	WHERE EXISTS (
		SELECT 1 FROM labels WHERE namespace = ? AND label = ? AND object_id = o.id
	);

INSERT OR REPLACE INTO labels (namespace, label, object_id) VALUES (?, ?, ?);
	`, namespace, label, namespace, label, objectId)
	} else {
		_, err = tx.Exec(`
UPDATE objects AS o
	SET ref_count = ref_count - 1
	WHERE EXISTS (
		SELECT 1 FROM labels WHERE namespace = ? AND label = ? AND object_id = o.id
	);

DELETE FROM labels WHERE namespace = ? AND label = ?;
`, namespace, label, namespace, label)
	}
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *hcasSession) CreateObject(data []byte, deps ...[]byte) ([]byte, error) {
	ow, err := s.StreamObject(deps...)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(ow, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	err = ow.Close()
	if err != nil {
		return nil, err
	}

	return ow.Name(), nil
}

func (s *hcasSession) StreamObject(deps ...[]byte) (ObjectWriter, error) {
	return createObjectStream(s, deps...)
}

func (s *hcasSession) Close() error {
	return cleanupSessionById(s.hcas, s.sessionId)
}

func cleanupSessionById(hcas *hcasInternal, sessionId int64) error {
	db := hcas.db
	rows, err := db.Query(
		"SELECT id FROM temp_files WHERE session_id = ?",
		sessionId,
	)
	if err != nil {
		return err
	}

	for rows.Next() {
		var tempFileId int64
		err = rows.Scan(&tempFileId)
		if err != nil {
			return err
		}

		// Attempt to delete temp file. It is possible it has already been deleted
		// in which case just move on.
		err = os.Remove(hcas.tempFilePath(tempFileId))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	err = rows.Close()
	if err != nil {
		return err
	}

	_, err = db.Exec(
		`
BEGIN;

-- Decrement referenced objects by session
UPDATE objects AS o SET ref_count = ref_count - 1 WHERE
	EXISTS (
		SELECT 1 FROM session_deps AS sd WHERE
			sd.session_id = ? AND sd.object_id = o.id
	);

DELETE FROM session_deps WHERE session_id = ?;
DELETE FROM temp_files WHERE session_id = ?;
DELETE FROM sessions WHERE id = ?;

COMMIT;
`, sessionId, sessionId, sessionId, sessionId)
	return err
}

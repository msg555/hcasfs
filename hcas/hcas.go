package hcas

import (
	"fmt"
	"bytes"
	"io"
	"os"
	"errors"
	"path/filepath"
	"database/sql"
	"crypto/sha256"
	"encoding/hex"
	"hash"

	_ "github.com/mattn/go-sqlite3"
)

const (
	VersionLatest = 1
)

const hcasSchemaInit = `
-- Setup and intialize HCAS schema version
CREATE TABLE IF NOT EXISTS version (
	version INTEGER
);

CREATE TABLE IF NOT EXISTS objects (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name BLOB NOT NULL,
	ref_count INTEGER NOT NULL
);
CREATE INDEX object_by_name ON objects(name);
CREATE INDEX object_by_ref_count ON objects(ref_count);

CREATE TABLE IF NOT EXISTS object_deps (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	parent_id INTEGER NOT NULL,
	child_id INTEGER NOT NULL,
	FOREIGN KEY (parent_id) REFERENCES objects(id) ON DELETE CASCADE,
	FOREIGN KEY (child_id) REFERENCES objects(id) ON DELETE CASCADE
);
CREATE INDEX object_deps_by_parent ON object_deps(parent_id, child_id);

CREATE TABLE IF NOT EXISTS sessions (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS session_deps (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER NOT NULL,
	object_id INTEGER NOT NULL,
	FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
	FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE
);
CREATE INDEX session_deps_by_session ON session_deps(session_id, object_id);

CREATE TABLE IF NOT EXISTS labels (
	label TEXT PRIMARY KEY,
	object_id INTEGER NOT NULL
);
`

type hcasInternal struct {
	version int64
	basePath string
	db *sql.DB
	sessions []Session
}

func NameHex(name []byte) string {
	return hex.EncodeToString(name)
}

func OpenHcas(basePath string) (Hcas, error) {
	basePath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", filepath.Join(basePath, "metadata.sqlite"))
	if err != nil {
		return nil, err
	}

	row := db.QueryRow("SELECT version FROM version;")

	var version int64
	err = row.Scan(&version)
	if err != nil {
		db.Close()
		return nil, err
	}

	if version != VersionLatest {
		db.Close()
		return nil, errors.New("unsupported hcas version")
	}

	return &hcasInternal{
		version: version,
		basePath: basePath,
		db: db,
		sessions: nil,
	}, nil
}

func CreateHcas(basePath string) (Hcas, error) {
	basePath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, err
	}

	err = os.Mkdir(basePath, 0o777)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", filepath.Join(basePath, "metadata.sqlite"))
	if err != nil {
		return nil, err
	}

	// Create a sample table
	_, err = db.Exec(hcasSchemaInit)
	if err != nil {
		db.Close()
		return nil, err
	}

	_, err = db.Exec("INSERT INTO version VALUES (?)", VersionLatest)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &hcasInternal{
		version: VersionLatest,
		basePath: basePath,
		db: db,
		sessions: nil,
	}, nil
}

func (h *hcasInternal) Close() error {
	var err error
	var errResult error
	for _, session := range h.sessions {
		err = session.Close()
		if errResult == nil {
			errResult = err
		}
	}
	err = h.db.Close()
	if errResult == nil {
		errResult = err
	}
	return errResult
}

type hcasSession struct {
	hcas *hcasInternal
	session_id int64
}

func (h *hcasInternal) CreateSession() (Session, error) {
	result, err := h.db.Exec("INSERT INTO sessions DEFAULT VALUES;")
	if err != nil {
		return nil, err
	}

	session_id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &hcasSession{
		hcas: h,
		session_id: session_id,
	}, nil
}

func (s* hcasSession) GetLabel(label string) ([]byte, error) {
	tx, err := s.hcas.db.Begin()
	if err != nil {
		return nil, err
	}

	row := tx.QueryRow(`
SELECT l.object_id, o.name FROM labels AS l
	JOIN objects AS o ON (l.object_id = o.id)
	WHERE label = ?;`, label)

	var object_id int64
	var name []byte
	err = row.Scan(&object_id, &name)
	if err != nil && err != sql.ErrNoRows {
		tx.Rollback()
		return nil, err
	}

	if name != nil {
		_, err = tx.Exec(`
INSERT INTO session_deps (session_id, object_id) VALUES (?, ?);
UPDATE objects SET ref_count = ref_count + 1 WHERE id = ?;
`, s.session_id, object_id, object_id)
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

func (s* hcasSession) SetLabel(label string, name []byte) error {
	tx, err := s.hcas.db.Begin()
	if err != nil {
		return err
	}

	// Lookup selected object
	var object_id int64
	if name != nil {
		row := tx.QueryRow(
			"SELECT id FROM objects WHERE name = ?",
			name,
		)

		err = row.Scan(&object_id)
		if err == sql.ErrNoRows {
			tx.Rollback()
			return errors.New("Object with name does not exist")
		} else if err != nil {
			tx.Rollback()
			return err
		}
	}

	// Delete existing labeled object if it existed
	if object_id != 0 {
		fmt.Printf("Set label %s to %d\n", label, object_id)
		_, err = tx.Exec(`
UPDATE objects AS o
	SET ref_count = ref_count - 1
	WHERE EXISTS (
		SELECT 1 FROM labels WHERE label = ? AND object_id = o.id
	);

INSERT OR REPLACE INTO labels (label, object_id) VALUES (?, ?);
	`, label, label, object_id)
	} else {
		_, err = tx.Exec(`
UPDATE objects AS o
	SET ref_count = ref_count - 1
	WHERE EXISTS (
		SELECT 1 FROM labels WHERE label = ? AND object_id = o.id
	);

DELETE FROM labels WHERE label = ?;
`, label, label)
	}
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s* hcasSession) ObjectOpen(name []byte) (*os.File, error) {
	return nil, nil
}

func (s* hcasSession) ObjectPath(name []byte) string {
	return ""
}

func (s* hcasSession) CreateObject(data []byte, deps ...[]byte) ([]byte, error) {
	ow := s.StreamObject(deps...)

	_, err := io.Copy(ow, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	err = ow.Close()
	if err != nil {
		return nil, err
	}

	return ow.Name(), nil
}

func (s* hcasSession) StreamObject(deps ...[]byte) ObjectWriter {
	// TODO: Is this really the best way to copy this?
	depsCopy := make([][]byte, len(deps))
	for _, dep := range deps {
		depCopy := make([]byte, len(dep))
		copy(depCopy, dep)
		depsCopy = append(depsCopy, depCopy)
	}

	return &hcasObjectWriter{
		session: s,
		hsh: sha256.New(),
		deps: depsCopy,
		name: nil,
	}
}

func (s* hcasSession) Close() error {
	_, err := s.hcas.db.Exec(
`
BEGIN;

-- Decrement referenced objects by session
UPDATE objects AS o SET ref_count = ref_count - 1 WHERE
	EXISTS (
		SELECT 1 FROM session_deps AS sd WHERE
			sd.session_id = ? AND sd.object_id = o.id
	);

DELETE FROM session_deps WHERE session_id = ?;
DELETE FROM sessions WHERE id = ?;

COMMIT;
`, s.session_id, s.session_id, s.session_id, s.session_id)
	return err
}

type hcasObjectWriter struct {
	session *hcasSession
	hsh hash.Hash
	deps [][]byte
	name []byte
}

func (ow *hcasObjectWriter) Write(p []byte) (n int, err error) {
	return ow.hsh.Write(p)
}

func (ow *hcasObjectWriter) Close() error {
	name := ow.hsh.Sum(nil)

	fmt.Printf("Try create object\n")
	tx, err := ow.session.hcas.db.Begin()
	if err != nil {
		return err
	}

	result, err := tx.Exec("INSERT INTO objects (name, ref_count) VALUES (?, 1)", name)
	if err != nil {
		tx.Rollback()
		return err
	}

	object_id, err := result.LastInsertId()
	fmt.Printf("MADE OBJECT %d\n", object_id)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(
		"INSERT INTO session_deps (session_id, object_id) VALUES (?, ?)",
		ow.session.session_id,
		object_id,
	)
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, dep := range ow.deps {
		row := tx.QueryRow("SELECT id FROM objects WHERE name = ?", dep)

		var dep_id int64
		err = row.Scan(&dep_id)
		if err == sql.ErrNoRows {
			tx.Rollback()
			return errors.New("Dependency does not exist")
		} else if err != nil {
			tx.Rollback()
			return err
		}

		_, err = tx.Exec(`
INSERT INTO object_deps (parent_id, child_id) VALUES (?, ?);
UPDATE objects SET ref_count = ref_count + 1 WHERE id = ?;
`, object_id, dep_id, dep_id)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	ow.name = name
	return nil
}

func (ow *hcasObjectWriter) Name() []byte {
	return ow.name
}

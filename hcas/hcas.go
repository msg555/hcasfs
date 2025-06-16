package hcas

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

const (
	VersionLatest = 1

	DataPath     = "data"
	TempPath     = "temp"
	MetadataPath = "metadata.sqlite"

	SqliteBusyTimeoutMs = 5000
)

const hcasSchemaInit = `
-- Setup and intialize HCAS schema version
CREATE TABLE IF NOT EXISTS version (
	version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name BLOB UNIQUE NOT NULL,
	ref_count INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS object_by_name ON objects(name);
CREATE INDEX IF NOT EXISTS object_by_ref_count ON objects(ref_count, id);

CREATE TABLE IF NOT EXISTS object_deps (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	parent_id INTEGER NOT NULL,
	child_id INTEGER NOT NULL,
	FOREIGN KEY (parent_id) REFERENCES objects(id),
	FOREIGN KEY (child_id) REFERENCES objects(id)
);
CREATE INDEX IF NOT EXISTS object_deps_by_parent ON object_deps(parent_id, child_id);

CREATE TABLE IF NOT EXISTS sessions (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS session_deps (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER NOT NULL,
	object_id INTEGER NOT NULL,
	FOREIGN KEY (session_id) REFERENCES sessions(id),
	FOREIGN KEY (object_id) REFERENCES objects(id)
);
CREATE INDEX IF NOT EXISTS session_deps_by_session ON session_deps(session_id, object_id);

CREATE TABLE IF NOT EXISTS temp_files (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER NOT NULL,
	FOREIGN KEY (session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS temp_objects (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS labels (
	namespace TEXT NOT NULL,
	label TEXT NOT NULL,
	object_id INTEGER NOT NULL,
	PRIMARY KEY (namespace, label)
);
`

type hcasInternal struct {
	version  int64
	basePath string
	db       *sql.DB
	sessions []Session
}

// Open an existing HCAS instance at the specified path
func OpenHcas(basePath string) (Hcas, error) {
	basePath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", filepath.Join(basePath, MetadataPath))
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", SqliteBusyTimeoutMs))
	if err != nil {
		return nil, err
	}

	var version int64
	err = db.QueryRow("SELECT version FROM version;").Scan(&version)
	if err != nil {
		db.Close()
		return nil, err
	}

	if version != VersionLatest {
		db.Close()
		return nil, errors.New("unsupported hcas version")
	}

	return &hcasInternal{
		version:  version,
		basePath: basePath,
		db:       db,
		sessions: nil,
	}, nil
}

// Create or open a new HCAS instance at the passed path
func CreateHcas(basePath string) (Hcas, error) {
	basePath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, err
	}

	err = os.Mkdir(basePath, 0o777)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}

	err = os.Mkdir(filepath.Join(basePath, TempPath), 0o777)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}

	err = os.Mkdir(filepath.Join(basePath, DataPath), 0o777)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}

	db, err := sql.Open("sqlite3", filepath.Join(basePath, MetadataPath))
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", SqliteBusyTimeoutMs))
	if err != nil {
		fmt.Println("wtf?")
		return nil, err
	}

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
		version:  VersionLatest,
		basePath: basePath,
		db:       db,
		sessions: nil,
	}, nil
}

func (h *hcasInternal) CreateSession() (Session, error) {
	return createSession(h)
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

func (h *hcasInternal) ObjectOpen(name Name) (*os.File, error) {
	return os.Open(h.ObjectPath(name))
}

func (h *hcasInternal) ObjectPath(name Name) string {
	nameHex := name.HexName()
	return filepath.Join(
		h.basePath,
		DataPath,
		nameHex[:2],
		nameHex[2:],
	)
}

func (h *hcasInternal) tempFilePath(tempFileId int64) string {
	return filepath.Join(h.basePath, TempPath, strconv.FormatInt(tempFileId, 10))
}

func (h *hcasInternal) dataFilePath(name Name) (string, string) {
	nameHex := name.HexName()
	dirPath := filepath.Join(h.basePath, DataPath, nameHex[:2])
	return dirPath, filepath.Join(dirPath, nameHex[2:])
}

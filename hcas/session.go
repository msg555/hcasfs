package hcas

import (
	"database/sql"
	"errors"
)

type hcasSession struct {
	hcas *hcasInternal
}

func createSession(hcas *hcasInternal) (Session, error) {
	return &hcasSession{
		hcas: hcas,
	}, nil
}

func (s *hcasSession) GetLabel(namespace string, label string) (*Name, error) {
	tx, err := s.hcas.db.Begin()
	if err != nil {
		return nil, err
	}

	row := tx.QueryRow(`
SELECT l.object_id, o.name FROM labels AS l
	JOIN objects AS o ON (l.object_id = o.id)
	WHERE namespace = ? AND label = ?;`, namespace, label)

	var objectId int64
	var nameBytes []byte
	err = row.Scan(&objectId, &nameBytes)
	if err != nil && err != sql.ErrNoRows {
		tx.Rollback()
		return nil, err
	}

	if nameBytes != nil {
		if len(nameBytes) != 32 {
			tx.Rollback()
			return nil, errors.New("unexpected object name from database")
		}

		_, err = tx.Exec(
			"UPDATE objects SET lease_time=? WHERE id=?",
			calculateLeaseTime(defaultObjectLease),
			objectId,
		)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	if nameBytes != nil {
		name := NewName(string(nameBytes))
		return &name, nil
	}
	return nil, nil
}

func (s *hcasSession) SetLabel(namespace string, label string, name *Name) error {
	tx, err := s.hcas.db.Begin()
	if err != nil {
		return err
	}

	// Lookup selected object
	var objectId int64
	if name != nil {
		row := tx.QueryRow(
			"SELECT id FROM objects WHERE name = ?",
			name.Name(),
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
		_, err = tx.Exec(`
UPDATE objects AS o
	SET ref_count = ref_count - 1
	WHERE EXISTS (
		SELECT 1 FROM labels WHERE namespace = ? AND label = ? AND object_id = o.id
	);

UPDATE objects SET ref_count = ref_count + 1
	WHERE id = ?;

INSERT OR REPLACE INTO labels (namespace, label, object_id) VALUES (?, ?, ?);
	`, namespace, label, objectId, namespace, label, objectId)
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

func (s *hcasSession) CreateObject(data []byte, deps ...Name) (*Name, error) {
	ow, err := createObjectStreamWithBuffer(s, data, deps...)
	if err != nil {
		return nil, err
	}

	err = ow.Close()
	if err != nil {
		return nil, err
	}

	return ow.Name(), nil
}

func (s *hcasSession) StreamObject(deps ...Name) (ObjectWriter, error) {
	return createObjectStream(s, deps...)
}

func (s *hcasSession) Close() error {
	return nil
}

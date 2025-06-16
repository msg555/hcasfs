package hcas

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
)

func (h *hcasInternal) GarbageCollect(iterations int) (bool, error) {
	const maxWorkPerIteration = 1000

	collectors := []func(int) (int, error){
		h.collectExpiredSessions,
		h.collectObjects,
		h.collectTempObjects,
	}

	complete := true
	for _, collector := range collectors {
		for budget := iterations; ; {
			workAmount := maxWorkPerIteration
			if budget > 0 && budget < workAmount {
				workAmount = budget
			}

			workDone, err := collector(workAmount)
			if err != nil {
				return false, err
			}
			if workDone == 0 {
				break
			} else if workDone == budget {
				complete = false
				break
			}
			if budget > 0 {
				budget -= workDone
			}
		}
	}
	return complete, nil
}

func (h *hcasInternal) collectExpiredSessions(amount int) (int, error) {
	rows, err := h.db.Query(`
SELECT id FROM sessions
	WHERE created_at < DATETIME('now', '-1 days')
	LIMIT ?`, amount)
	if err != nil {
		return 0, err
	}

	sessionIds := make([]int64, 0, amount)
	for rows.Next() {
		var sessionId int64
		err = rows.Scan(&sessionId)
		if err != nil {
			return 0, err
		}
		sessionIds = append(sessionIds, sessionId)
	}

	err = rows.Err()
	if err != nil {
		return 0, err
	}

	err = rows.Close()
	if err != nil {
		return 0, err
	}

	result := 0
	for _, sessionId := range sessionIds {
		err = cleanupSessionById(h, sessionId)
		if err != nil {
			slog.Warn("failed to collect session", "sessionId", sessionId)
		} else {
			result++
		}
	}

	if result > 0 {
		slog.Info("Collected sessions", "count", result)
	}
	return result, nil
}

func (h *hcasInternal) collectTempObjects(amount int) (int, error) {
	// Need to hold exclusive lock while cleaning files out of data/
	_, err := h.db.Exec(`
CREATE TEMP TABLE temp_objects_to_delete (
	id INTEGER PRIMARY KEY,
	name BLOB NOT NULL,
	object_exists INTEGER DEFAULT 0
)`)
	if err != nil {
		return 0, err
	}

	_, err = h.db.Exec(`
BEGIN IMMEDIATE;

-- Capture the set of temp objects being deleted
INSERT INTO temp_objects_to_delete (id, name)
	SELECT id, name FROM temp_objects
	ORDER BY id LIMIT ?;

-- Remove all temp objects to delete from temp_objects
DELETE FROM temp_objects AS tos WHERE EXISTS (
	SELECT 1 FROM temp_objects_to_delete AS totd
	WHERE tos.id = totd.id
);

-- Remove from the objects to delete objects that actually exist
UPDATE temp_objects_to_delete AS totd SET
	object_exists = 1
WHERE EXISTS (
	SELECT 1 FROM objects AS o WHERE totd.name = o.name
);`, amount)
	if err != nil {
		return 0, err
	}

	rows, err := h.db.Query("SELECT name FROM temp_objects_to_delete WHERE object_exists = 0")
	if err != nil {
		h.db.Exec("ROLLBACK")
		return 0, err
	}

	for rows.Next() {
		var nameBytes []byte
		err = rows.Scan(&nameBytes)
		if err != nil {
			h.db.Exec("ROLLBACK")
			return 0, err
		}
		if len(nameBytes) != 32 {
			h.db.Exec("ROLLBACK")
			return 0, errors.New("got invalid name from db")
		}
		name := NewName(string(nameBytes))

		// Attempt to delete object data. It may already not exist so ignore on
		// failures.
		_, objectPath := h.dataFilePath(name)
		err = os.Remove(objectPath)
		if err != nil && !os.IsNotExist(err) {
			h.db.Exec("ROLLBACK")
			return 0, err
		}
	}

	err = rows.Err()
	if err != nil {
		h.db.Exec("ROLLBACK")
		return 0, err
	}

	err = rows.Close()
	if err != nil {
		h.db.Exec("ROLLBACK")
		return 0, err
	}

	_, err = h.db.Exec("COMMIT")
	if err != nil {
		return 0, err
	}

	row := h.db.QueryRow("SELECT COUNT(1) FROM temp_objects_to_delete")
	var objectsDeleted int
	err = row.Scan(&objectsDeleted)
	if err != nil {
		return 0, err
	}

	_, err = h.db.Exec("DROP TABLE temp_objects_to_delete")
	if err != nil {
		return 0, nil
	}

	if objectsDeleted > 0 {
		slog.Info("Collected temp objects", "count", objectsDeleted)
	}
	return objectsDeleted, nil
}

func (h *hcasInternal) collectObjects(amount int) (int, error) {
	fmt.Printf("Collecting up to %d objects\n", amount)

	_, err := h.db.Exec(`
CREATE TEMP TABLE objects_to_delete (
	id INTEGER PRIMARY KEY,
	name BLOB NOT NULL
)`)
	if err != nil {
		return 0, err
	}

	_, err = h.db.Exec(`
BEGIN IMMEDIATE;

-- Capture the set of objects being deleted
INSERT INTO objects_to_delete (id, name)
	SELECT id, name FROM objects WHERE ref_count = 0
	ORDER BY id LIMIT ?;

-- Update the ref counts of things they reference
WITH ref_changes AS (
	SELECT od.child_id, COUNT(1) AS amount FROM objects_to_delete AS o
	JOIN object_deps AS od ON (o.id = od.parent_id)
	GROUP BY od.child_id
)
UPDATE objects
SET ref_count = ref_count - ref_changes.amount
FROM ref_changes
WHERE objects.id = ref_changes.child_id;

-- Delete their references
DELETE FROM object_deps AS od
WHERE EXISTS (
	SELECT 1 FROM objects_to_delete AS tos WHERE od.parent_id = tos.id
);

-- Delete the objects
DELETE FROM objects AS o
WHERE EXISTS (
	SELECT 1 FROM objects_to_delete AS tos WHERE o.id = tos.id
);

-- Move their data files into temp_files
INSERT INTO temp_objects (name)
	SELECT name FROM objects_to_delete;

COMMIT;
`, amount)

	if err != nil {
		h.db.Exec("DROP TABLE objects_to_delete")
		return 0, err
	}

	var rowCount int
	err = h.db.QueryRow("SELECT COUNT(1) FROM objects_to_delete").Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	_, err = h.db.Exec("DROP TABLE objects_to_delete")
	if err != nil {
		return 0, err
	}

	if rowCount > 0 {
		slog.Info("Collected objects", "count", rowCount)
	}
	return rowCount, nil
}

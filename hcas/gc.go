package hcas

import (
	_ "errors"
	"fmt"
	"log/slog"
	_ "os"
)

func (h *hcasInternal) GarbageCollect(iterations int) (bool, error) {
	const maxWorkPerIteration = 1000

	collectors := []func(int) (int, error){
		h.collectObjects,
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

func (h *hcasInternal) collectObjects(amount int) (int, error) {
	fmt.Printf("Collecting up to %d objects\n", amount)

	// Maybe I still need temp objects?
	_, err := h.db.Exec(`
CREATE TEMP TABLE objects_to_delete (
	id INTEGER PRIMARY KEY,
	name BLOB NOT NULL
)`)
	if err != nil {
		return 0, err
	}

	expiredLeaseTime := calculateLeaseTime(0)
	_, err = h.db.Exec(`
BEGIN IMMEDIATE;

-- Capture the set of objects being deleted
INSERT INTO objects_to_delete (id, name)
	SELECT id, name FROM objects
	WHERE ref_count = 0 AND lease_time < ?
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
`, expiredLeaseTime, amount)

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

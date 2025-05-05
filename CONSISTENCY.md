The following consistency criteria should be maintained at all times
(uncommitted transactions exempt) to ensure faithful operation of HCAS.
It is enough for any repair/consistency tool to verify these rules to
ensure HCAS is in a consistent place.

These constraints are on top of the SQL-level constraints that sqlite itself
should enforce.

## Data integrity

If a file with name N exists in the data/ directory then it must be fully
formed; its content matching the content used to generate that name N.

## Object implies data

If an object with name N exists in the committed objects table, a file with name
N must exist in the data/ directory.

## Reference count consistency

Each object's ref_count column should exactly equal the number of refrences in
the object_deps, session_deps, and labels tables. An object with ref_count 0 can
be deleted freely.

## Data file tracking

If a file with name N exists in the data/ directory then either a row with
matching name must exist in either the objects or temp_objects table. This
ensures that object data can be cleaned up.

## Temp file tracking

If a temporary file with name T exists in the tmp/ directory then there must be
a row in the temp_files table with matching name T. This ensures that data
being written into the system can be cleaned up if the write is interrupted.
Temporary files are associated with sessions and must be cleaned up at the same
time as the parent session is cleaned up.

## Session expiration

Sessions expire 24 hours after creation. Sessions are not meant for long term
usage.

## Data directory protection

Files cannot be linked/unlinked into the data section unless the database
exclusive lock is held.

## Out of scope - data directory cleanup

HCAS may create a hierarchy of directories for data files using e.g. the first
few bytes of the object names. These directories do not need to be cleaned up
even if all contained files get removed.

module github.com/msg555/hcas

go 1.23.0

toolchain go1.24.2

require (
	bazil.org/fuse v0.0.0-20230120002735-62a210ff1fd5
	github.com/go-errors/errors v1.5.1
	github.com/mattn/go-sqlite3 v1.14.28
	github.com/stretchr/testify v1.10.0
	golang.org/x/sys v0.33.0
)

replace bazil.org/fuse => github.com/msg555/fuse v0.0.0-20250705003504-9e595cac7919

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

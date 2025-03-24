package k3SQLServer

import "errors"

func CreateTable(query *K3CreateQuery) error {
	if databaseExists(query.Database) {
		if !existsTable(query.Table, query.Database) {
			return createTableFile(query)
		}
		return errors.New("table already exists")
	}
	return errors.New("database does not exists")
}

func InsertTable(query *K3InsertQuery) error {
	if databaseExists(query.Database) {
		if existsTable(query.Table, query.Database) {
			return insertTableFile(query)
		}
		return errors.New("table does not exists")
	}
	return errors.New("database does not exists")
}

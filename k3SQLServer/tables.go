package k3SQLServer

import "errors"

func CreateTable(query *k3CreateQuery) error {
	if databaseExists(query.database) {
		if !existsTable(query.table, query.database) {
			return createTableFile(query)
		}
		return errors.New("table already exists")
	}
	return errors.New("database does not exists")
}

func InsertTable(query *k3InsertQuery) error {
	if databaseExists(query.database) {
		if existsTable(query.table, query.database) {
			return insertTableFile(query)
		}
		return errors.New("table does not exists")
	}
	return errors.New("database does not exists")
}

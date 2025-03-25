package k3SQLServer

import "errors"

func createTable(query *k3CreateQuery) error {
	if databaseExists(query.table.database) {
		if !existsTable(query.table) {
			return createTableFile(query)
		}
		return errors.New(tableExists)
	}
	return errors.New(databaseNotExists)
}

func insertTable(query *k3InsertQuery) error {
	if databaseExists(query.table.database) {
		if existsTable(query.table) {
			return insertTableFile(query)
		}
		return errors.New(tableNotExists)
	}
	return errors.New(databaseNotExists)
}

func selectTable(query *k3SelectQuery) error {
	if databaseExists(query.table.database) {
		if existsTable(query.table) {
			return selectTableFile(query)
		}
		return errors.New(tableNotExists)
	}
	return errors.New(databaseNotExists)
}

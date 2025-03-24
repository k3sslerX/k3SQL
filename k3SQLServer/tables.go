package k3SQLServer

import "errors"

const tableNotExists = "table does not exists"
const tableExists = "table already exists"
const databaseNotExists = "database does not exists"

func createTable(query *k3CreateQuery) error {
	if databaseExists(query.database) {
		if !existsTable(query.table, query.database) {
			return createTableFile(query)
		}
		return errors.New(tableExists)
	}
	return errors.New(databaseNotExists)
}

func insertTable(query *k3InsertQuery) error {
	if databaseExists(query.database) {
		if existsTable(query.table, query.database) {
			return insertTableFile(query)
		}
		return errors.New(tableNotExists)
	}
	return errors.New(databaseNotExists)
}

func selectTable(query *k3SelectQuery) error {
	if databaseExists(query.database) {
		if existsTable(query.table, query.database) {
			return nil
		}
		return errors.New(tableNotExists)
	}
	return errors.New(databaseNotExists)
}

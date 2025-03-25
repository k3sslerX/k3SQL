package k3SQLServer

import "errors"

func createTable(query *k3CreateQuery) error {
	if databaseExists(query.table.database) {
		if !existsTable(query.table) {
			err := createTableFile(query)
			if err == nil {
				k3Tables[query.table.name] = query.table
			}
			return err
		}
		return errors.New(tableAlreadyExists)
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

func selectTable(query *k3SelectQuery) ([]map[string]string, error) {
	if databaseExists(query.table.database) {
		if existsTable(query.table) {
			resp, err := selectTableFile(query)
			return resp, err
		}
		return nil, errors.New(tableNotExists)
	}
	return nil, errors.New(databaseNotExists)
}

func dropTable(table *k3Table) error {
	if databaseExists(table.database) {
		if existsTable(table) {
			err := dropTableFile(table)
			if err == nil {
				delete(k3Tables, table.name)
			}
			return err
		}
		return errors.New(tableNotExists)
	}
	return errors.New(databaseNotExists)
}

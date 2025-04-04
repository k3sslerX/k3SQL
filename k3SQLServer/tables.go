package k3SQLServer

import "errors"

func createTable(query *k3CreateQuery) error {
	if databaseExists(query.table.database) {
		if !existsTable(query.table) {
			err := createTableFile(query)
			if err == nil {
				k3Tables[query.table.database+"."+query.table.name] = query.table
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

func updateTable(query *k3UpdateQuery) (int, error) {
	if databaseExists(query.table.database) {
		if existsTable(query.table) {
			if query.table.name == "users" {
				return 0, errors.New(accessDenied)
			}
			return updateTableFile(query)
		}
		return 0, errors.New(tableNotExists)
	}
	return 0, errors.New(databaseNotExists)
}

func deleteTable(query *k3DeleteQuery) (int, error) {
	if databaseExists(query.table.database) {
		if existsTable(query.table) {
			if query.table.name == "users" {
				return 0, errors.New(accessDenied)
			}
			return deleteTableFile(query)
		}
		return 0, errors.New(tableNotExists)
	}
	return 0, errors.New(databaseNotExists)
}

func dropTable(table *k3Table) error {
	if databaseExists(table.database) {
		if existsTable(table) {
			if table.name == "users" {
				return errors.New(accessDenied)
			}
			err := dropTableFile(table)
			if err == nil {
				delete(k3Tables, table.database+"."+table.name)
			}
			return err
		}
		return errors.New(tableNotExists)
	}
	return errors.New(databaseNotExists)
}

func processUser(userQuery *k3UserQuery) error {
	if databaseExists(userQuery.database) {
		if userQuery.username == "k3user" {
			return errors.New(accessDenied)
		}
		values := make([]map[string]string, 1)
		values[0] = map[string]string{
			"name":     userQuery.username,
			"password": userQuery.password,
		}
		if userQuery.action == k3CREATE {
			insertQuery := &k3InsertQuery{
				table:  k3Tables[userQuery.database+".users"],
				values: values,
			}
			return insertTableFile(insertQuery)
		} else if userQuery.action == k3DELETE {
			cond := make([]k3Condition, 1)
			cond[0] = k3Condition{
				column:   "name",
				operator: "=",
				value:    userQuery.username,
			}
			deleteQuery := &k3DeleteQuery{
				table:      k3Tables[userQuery.database+".users"],
				conditions: cond,
			}
			n, err := deleteTableFile(deleteQuery)
			if n == 0 {
				return errors.New(userNotFound)
			}
			return err
		}
	}
	return errors.New(databaseNotExists)
}

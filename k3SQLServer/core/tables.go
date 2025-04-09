package core

import (
	"errors"
)

func CreateTable(query *K3CreateQuery) error {
	if DatabaseExists(query.Table.Database) {
		if !ExistsTable(query.Table) {
			err := CreateTableFile(query)
			if err == nil {
				K3Tables[query.Table.Database+"."+query.Table.Name] = query.Table
			}
			return err
		}
		return errors.New(TableAlreadyExists)
	}
	return errors.New(DatabaseNotExists)
}

func InsertTable(query *K3InsertQuery) error {
	if DatabaseExists(query.Table.Database) {
		if ExistsTable(query.Table) {
			return InsertTableFile(query)
		}
		return errors.New(TableNotExists)
	}
	return errors.New(DatabaseNotExists)
}

func SelectTable(query *K3SelectQuery) ([]map[string]string, int, error) {
	if DatabaseExists(query.Table.Database) {
		if ExistsTable(query.Table) {
			resp, rows, err := SelectTableFile(query)
			return resp, rows, err
		}
		return nil, 0, errors.New(TableNotExists)
	}
	return nil, 0, errors.New(DatabaseNotExists)
}

func UpdateTable(query *K3UpdateQuery) (int, error) {
	if DatabaseExists(query.Table.Database) {
		if ExistsTable(query.Table) {
			if query.Table.Name == "users" {
				return 0, errors.New(AccessDenied)
			}
			return UpdateTableFile(query)
		}
		return 0, errors.New(TableNotExists)
	}
	return 0, errors.New(DatabaseNotExists)
}

func DeleteTable(query *K3DeleteQuery) (int, error) {
	if DatabaseExists(query.Table.Database) {
		if ExistsTable(query.Table) {
			if query.Table.Name == "users" {
				return 0, errors.New(AccessDenied)
			}
			return DeleteTableFile(query)
		}
		return 0, errors.New(TableNotExists)
	}
	return 0, errors.New(DatabaseNotExists)
}

func DropTable(table *K3Table) error {
	if DatabaseExists(table.Database) {
		if ExistsTable(table) {
			if table.Name == "users" {
				return errors.New(AccessDenied)
			}
			err := DropTableFile(table)
			if err == nil {
				delete(K3Tables, table.Database+"."+table.Name)
			}
			return err
		}
		return errors.New(TableNotExists)
	}
	return errors.New(DatabaseNotExists)
}

func ProcessUser(userQuery *K3UserQuery) error {
	if DatabaseExists(userQuery.Database) {
		if userQuery.Username == "k3user" {
			return errors.New(AccessDenied)
		}
		values := make([]map[string]string, 1)
		values[0] = map[string]string{
			"name":     userQuery.Username,
			"password": userQuery.Password,
		}
		if userQuery.Action == K3CREATE {
			insertQuery := &K3InsertQuery{
				Table:  K3Tables[userQuery.Database+".users"],
				Values: values,
			}
			return InsertTableFile(insertQuery)
		} else if userQuery.Action == K3DELETE {
			cond := make([]K3Condition, 1)
			cond[0] = K3Condition{
				Column:   "name",
				Operator: "=",
				Value:    userQuery.Username,
			}
			deleteQuery := &K3DeleteQuery{
				Table:      K3Tables[userQuery.Database+".users"],
				Conditions: cond,
			}
			n, err := DeleteTableFile(deleteQuery)
			if n == 0 {
				return errors.New(UserNotFound)
			}
			return err
		}
	}
	return errors.New(DatabaseNotExists)
}

package core

import (
	"errors"
	"k3SQLServer/shared"
	"k3SQLServer/storage"
	"strconv"
)

func checkPermission(table *shared.K3Table, user string, permission int) bool {
	if user == shared.CoreUser {
		return true
	}
	approve := false
	conditions := make([]shared.K3Condition, 2)
	conditions[0] = shared.K3Condition{
		Column:   "user",
		Operator: "=",
		Value:    user,
	}
	conditions[1] = shared.K3Condition{
		Column:   "table",
		Operator: "=",
		Value:    table.Name,
	}
	selectPermissions := shared.K3SelectQuery{
		Table:      shared.K3Tables[table.Database+"."+shared.K3PermissionsTable],
		Values:     []string{"permission"},
		Conditions: conditions,
	}
	values, _, err := storage.SelectTableFile(&selectPermissions)
	if err == nil {
		for _, value := range values {
			perm, ok := value["permission"]
			if ok {
				permInt, err := strconv.Atoi(perm)
				if err == nil {
					if permission >= permInt {
						approve = true
					}
				}
			}
		}
	}
	return approve
}

func CreateTable(query *shared.K3CreateQuery) error {
	if storage.DatabaseExists(query.Table.Database) {
		if !storage.ExistsTable(query.Table) {
			err := storage.CreateTableFile(query)
			if err == nil {
				shared.K3Tables[query.Table.Database+"."+query.Table.Name] = query.Table
				insertValues := make([]map[string]string, 1)
				insertValues[0] = make(map[string]string, 1)
				insertValues[0]["table"] = query.Table.Name
				insertQuery := shared.K3InsertQuery{
					Table:  shared.K3Tables[query.Table.Database+"."+shared.K3TablesTable],
					Values: insertValues,
				}
				err = GrantPermission(query.Table, "k3user", shared.K3All)
				if err == nil {
					err = InsertTable(&insertQuery, shared.CoreUser)
				}
			}
			return err
		}
		return errors.New(shared.TableAlreadyExists)
	}
	return errors.New(shared.DatabaseNotExists)
}

func InsertTable(query *shared.K3InsertQuery, user string) error {
	if storage.DatabaseExists(query.Table.Database) {
		if storage.ExistsTable(query.Table) {
			if checkPermission(query.Table, user, shared.K3Write) {
				return storage.InsertTableFile(query)
			}
			return errors.New(shared.AccessDenied)
		}
		return errors.New(shared.TableNotExists)
	}
	return errors.New(shared.DatabaseNotExists)
}

func SelectTable(query *shared.K3SelectQuery, user string) ([]map[string]string, int, error) {
	if storage.DatabaseExists(query.Table.Database) {
		if storage.ExistsTable(query.Table) {
			if checkPermission(query.Table, user, shared.K3Read) {
				resp, rows, err := storage.SelectTableFile(query)
				return resp, rows, err
			}
			return nil, 0, errors.New(shared.AccessDenied)
		}
		return nil, 0, errors.New(shared.TableNotExists)
	}
	return nil, 0, errors.New(shared.DatabaseNotExists)
}

func UpdateTable(query *shared.K3UpdateQuery, user string) (int, error) {
	if storage.DatabaseExists(query.Table.Database) {
		if storage.ExistsTable(query.Table) {
			if query.Table.Name == shared.K3UsersTable || query.Table.Name == shared.K3TablesTable {
				return 0, errors.New(shared.AccessDenied)
			}
			if checkPermission(query.Table, user, shared.K3Write) {
				return storage.UpdateTableFile(query)
			}
			return 0, errors.New(shared.AccessDenied)
		}
		return 0, errors.New(shared.TableNotExists)
	}
	return 0, errors.New(shared.DatabaseNotExists)
}

func DeleteTable(query *shared.K3DeleteQuery, user string) (int, error) {
	if storage.DatabaseExists(query.Table.Database) {
		if storage.ExistsTable(query.Table) {
			if query.Table.Name == shared.K3UsersTable || query.Table.Name == shared.K3TablesTable {
				return 0, errors.New(shared.AccessDenied)
			}
			if checkPermission(query.Table, user, shared.K3Write) {
				return storage.DeleteTableFile(query)
			}
			return 0, errors.New(shared.AccessDenied)
		}
		return 0, errors.New(shared.TableNotExists)
	}
	return 0, errors.New(shared.DatabaseNotExists)
}

func DropTable(table *shared.K3Table, user string) error {
	if storage.DatabaseExists(table.Database) {
		if storage.ExistsTable(table) {
			if table.Name == shared.K3UsersTable || table.Name == shared.K3TablesTable {
				return errors.New(shared.AccessDenied)
			}
			if checkPermission(table, user, shared.K3Write) {
				conditionsTables := make([]shared.K3Condition, 1)
				conditionsPermissions := make([]shared.K3Condition, 1)
				conditionTables := shared.K3Condition{
					Column:   "table",
					Value:    table.Name,
					Operator: "=",
				}
				conditionPermissions := shared.K3Condition{
					Column:   "table",
					Value:    table.Name,
					Operator: "=",
				}
				conditionsTables[0] = conditionTables
				conditionsPermissions[0] = conditionPermissions
				queryTables := shared.K3DeleteQuery{
					Table:      shared.K3Tables[table.Database+"."+shared.K3TablesTable],
					Conditions: conditionsTables,
				}
				queryPermissions := shared.K3DeleteQuery{
					Table:      shared.K3Tables[table.Database+"."+shared.K3PermissionsTable],
					Conditions: conditionsPermissions,
				}
				_, err := storage.DeleteTableFile(&queryTables)
				if err == nil {
					_, err = storage.DeleteTableFile(&queryPermissions)
					if err == nil {
						err = storage.DropTableFile(table)
						if err == nil {
							delete(shared.K3Tables, table.Database+"."+table.Name)
						}
					}
				}
				return err
			}
			return errors.New(shared.AccessDenied)
		}
		return errors.New(shared.TableNotExists)
	}
	return errors.New(shared.DatabaseNotExists)
}

func ProcessUser(userQuery *shared.K3UserQuery) error {
	if storage.DatabaseExists(userQuery.Database) {
		if userQuery.Username == "k3user" || userQuery.Username == shared.CoreUser {
			return errors.New(shared.AccessDenied)
		}
		values := make([]map[string]string, 1)
		values[0] = map[string]string{
			"name":     userQuery.Username,
			"password": userQuery.Password,
		}
		if userQuery.Action == shared.K3CREATE {
			insertQuery := &shared.K3InsertQuery{
				Table:  shared.K3Tables[userQuery.Database+"."+shared.K3UsersTable],
				Values: values,
			}
			return storage.InsertTableFile(insertQuery)
		} else if userQuery.Action == shared.K3DELETE {
			cond := make([]shared.K3Condition, 1)
			cond[0] = shared.K3Condition{
				Column:   "name",
				Operator: "=",
				Value:    userQuery.Username,
			}
			deleteQuery := &shared.K3DeleteQuery{
				Table:      shared.K3Tables[userQuery.Database+"."+shared.K3UsersTable],
				Conditions: cond,
			}
			n, err := storage.DeleteTableFile(deleteQuery)
			if n == 0 {
				return errors.New(shared.UserNotFound)
			}
			return err
		}
	}
	return errors.New(shared.DatabaseNotExists)
}

func GrantPermission(table *shared.K3Table, user string, permission int) error {
	values := make([]map[string]string, 1)
	values[0] = map[string]string{
		"user":       user,
		"table":      table.Name,
		"permission": strconv.Itoa(permission),
	}
	permissionQuery := shared.K3InsertQuery{
		Table:  shared.K3Tables[table.Database+"."+shared.K3PermissionsTable],
		Values: values,
	}
	return storage.InsertTableFile(&permissionQuery)
}

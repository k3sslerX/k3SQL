package core

import (
	"errors"
	"strconv"
)

func checkPermission(table *K3Table, user string, permission int) bool {
	approve := false
	conditions := make([]K3Condition, 2)
	conditions[0] = K3Condition{
		Column:   "user",
		Operator: "=",
		Value:    user,
	}
	conditions[1] = K3Condition{
		Column:   "table",
		Operator: "=",
		Value:    table.Name,
	}
	selectPermissions := K3SelectQuery{
		Table:      table,
		Values:     []string{"permission"},
		Conditions: conditions,
	}
	values, _, err := SelectTableFile(&selectPermissions)
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

func CreateTable(query *K3CreateQuery) error {
	if DatabaseExists(query.Table.Database) {
		if !ExistsTable(query.Table) {
			err := CreateTableFile(query)
			if err == nil {
				K3Tables[query.Table.Database+"."+query.Table.Name] = query.Table
				insertValues := make([]map[string]string, 1)
				insertValues[0] = make(map[string]string, 1)
				insertValues[0]["table"] = query.Table.Name
				insertQuery := K3InsertQuery{
					Table:  K3Tables[query.Table.Database+"."+K3TablesTable],
					Values: insertValues,
				}
				err = InsertTable(&insertQuery, "k3user")
				if err == nil {
					err = GrantPermission(query.Table, "k3user", K3All)
				}
			}
			return err
		}
		return errors.New(TableAlreadyExists)
	}
	return errors.New(DatabaseNotExists)
}

func InsertTable(query *K3InsertQuery, user string) error {
	if DatabaseExists(query.Table.Database) {
		if ExistsTable(query.Table) {
			if checkPermission(query.Table, user, K3Write) {
				return InsertTableFile(query)
			}
			return errors.New(AccessDenied)
		}
		return errors.New(TableNotExists)
	}
	return errors.New(DatabaseNotExists)
}

func SelectTable(query *K3SelectQuery, user string) ([]map[string]string, int, error) {
	if DatabaseExists(query.Table.Database) {
		if ExistsTable(query.Table) {
			if checkPermission(query.Table, user, K3Read) {
				resp, rows, err := SelectTableFile(query)
				return resp, rows, err
			}
			return nil, 0, errors.New(AccessDenied)
		}
		return nil, 0, errors.New(TableNotExists)
	}
	return nil, 0, errors.New(DatabaseNotExists)
}

func UpdateTable(query *K3UpdateQuery, user string) (int, error) {
	if DatabaseExists(query.Table.Database) {
		if ExistsTable(query.Table) {
			if query.Table.Name == K3UsersTable || query.Table.Name == K3TablesTable {
				return 0, errors.New(AccessDenied)
			}
			if checkPermission(query.Table, user, K3Write) {
				return UpdateTableFile(query)
			}
			return 0, errors.New(AccessDenied)
		}
		return 0, errors.New(TableNotExists)
	}
	return 0, errors.New(DatabaseNotExists)
}

func DeleteTable(query *K3DeleteQuery, user string) (int, error) {
	if DatabaseExists(query.Table.Database) {
		if ExistsTable(query.Table) {
			if query.Table.Name == K3UsersTable || query.Table.Name == K3TablesTable {
				return 0, errors.New(AccessDenied)
			}
			if checkPermission(query.Table, user, K3Write) {
				return DeleteTableFile(query)
			}
			return 0, errors.New(AccessDenied)
		}
		return 0, errors.New(TableNotExists)
	}
	return 0, errors.New(DatabaseNotExists)
}

func DropTable(table *K3Table, user string) error {
	if DatabaseExists(table.Database) {
		if ExistsTable(table) {
			if table.Name == K3UsersTable || table.Name == K3TablesTable {
				return errors.New(AccessDenied)
			}
			if checkPermission(table, user, K3Write) {
				conditionsTables := make([]K3Condition, 1)
				conditionsPermissions := make([]K3Condition, 1)
				conditionTables := K3Condition{
					Column:   "table",
					Value:    table.Name,
					Operator: "=",
				}
				conditionPermissions := K3Condition{
					Column:   "table",
					Value:    table.Name,
					Operator: "=",
				}
				conditionsTables[0] = conditionTables
				conditionsPermissions[0] = conditionPermissions
				queryTables := K3DeleteQuery{
					Table:      K3Tables[table.Database+"."+K3TablesTable],
					Conditions: conditionsTables,
				}
				queryPermissions := K3DeleteQuery{
					Table:      K3Tables[table.Database+"."+K3PermissionsTable],
					Conditions: conditionsPermissions,
				}
				_, err := DeleteTableFile(&queryTables)
				if err == nil {
					_, err = DeleteTableFile(&queryPermissions)
					if err == nil {
						err = DropTableFile(table)
						if err == nil {
							delete(K3Tables, table.Database+"."+table.Name)
						}
					}
				}
				return err
			}
			return errors.New(AccessDenied)
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
				Table:  K3Tables[userQuery.Database+"."+K3UsersTable],
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
				Table:      K3Tables[userQuery.Database+"."+K3UsersTable],
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

func GrantPermission(table *K3Table, user string, permission int) error {
	values := make([]map[string]string, 1)
	values[0] = map[string]string{
		"user":       user,
		"table":      table.Name,
		"permission": strconv.Itoa(permission),
	}
	permissionQuery := K3InsertQuery{
		Table:  K3Tables[table.Database+"."+K3PermissionsTable],
		Values: values,
	}
	return InsertTableFile(&permissionQuery)
}

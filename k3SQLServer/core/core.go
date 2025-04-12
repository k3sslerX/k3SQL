package core

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

func init() {
	K3Tables = make(map[string]*K3Table, 1)
	initialised := false
	_, err := os.Stat(K3FilesPath)
	if err == nil {
		initialised = true
	}
	if !initialised {
		err = os.MkdirAll(K3FilesPath, os.ModePerm)
		if err == nil {
			err1 := os.MkdirAll(K3DataPath, os.ModePerm)
			err2 := os.MkdirAll(K3ConfigurationPath, os.ModePerm)
			if err1 == nil && err2 == nil {
				err = CreateDatabase("k3db")
			}
		}
	}
}

func CreateDatabase(name string) error {
	if !DatabaseExists(name) {
		err := CreateDatabaseFile(name)
		if err == nil {
			userTableFields := make([]string, 2)
			tablesTableFields := make([]string, 1)
			permissionsTableFields := make([]string, 3)
			userTableFields[0], userTableFields[1] = "name", "password"
			tablesTableFields[0] = "table"
			permissionsTableFields[0] = "user"
			permissionsTableFields[1] = "table"
			permissionsTableFields[2] = "permission"
			userTable := K3Table{
				Database: name,
				Name:     K3UsersTable,
				Fields:   userTableFields,
				Mu:       new(sync.RWMutex),
			}
			tablesTable := K3Table{
				Database: name,
				Name:     K3TablesTable,
				Fields:   tablesTableFields,
				Mu:       new(sync.RWMutex),
			}
			permissionsTable := K3Table{
				Database: name,
				Name:     K3PermissionsTable,
				Fields:   permissionsTableFields,
				Mu:       new(sync.RWMutex),
			}
			queryUsersFields := make(map[string]int, 2)
			queryTablesFields := make(map[string]int, 1)
			queryPermissionsFields := make(map[string]int, 3)
			queryTablesFields["table"] = K3TEXT
			queryUsersFields["name"] = K3TEXT
			queryUsersFields["password"] = K3TEXT
			queryPermissionsFields["user"] = K3TEXT
			queryPermissionsFields["table"] = K3TEXT
			queryPermissionsFields["permission"] = K3INT
			queryUsers := K3CreateQuery{
				Table:  &userTable,
				Fields: queryUsersFields,
			}
			queryTables := K3CreateQuery{
				Table:  &tablesTable,
				Fields: queryTablesFields,
			}
			queryPermissions := K3CreateQuery{
				Table:  &permissionsTable,
				Fields: queryPermissionsFields,
			}
			err = CreateTableFile(&queryTables)
			if err != nil {
				return err
			}
			err = CreateTableFile(&queryUsers)
			if err != nil {
				return err
			}
			err = CreateTableFile(&queryPermissions)
			if err != nil {
				return err
			}
			insertPermissionsValues := make([]map[string]string, 3)
			insertPermissionsValues[0] = make(map[string]string, 3)
			insertPermissionsValues[1] = make(map[string]string, 3)
			insertPermissionsValues[2] = make(map[string]string, 3)
			insertPermissionsValues[0]["user"] = "k3user"
			insertPermissionsValues[0]["table"] = K3UsersTable
			insertPermissionsValues[0]["permission"] = strconv.Itoa(K3Read)
			insertPermissionsValues[1]["user"] = "k3user"
			insertPermissionsValues[1]["table"] = K3TablesTable
			insertPermissionsValues[1]["permission"] = strconv.Itoa(K3Read)
			insertPermissionsValues[2]["user"] = "k3user"
			insertPermissionsValues[2]["table"] = K3PermissionsTable
			insertPermissionsValues[2]["permission"] = strconv.Itoa(K3All)
			insertPermissionsQuery := K3InsertQuery{
				Table:  &permissionsTable,
				Values: insertPermissionsValues,
			}
			err = InsertTableFile(&insertPermissionsQuery)
			insertUsersValues := make([]map[string]string, 1)
			insertUsersValues[0] = make(map[string]string, 2)
			insertUsersValues[0]["name"] = "k3user"
			insertUsersValues[0]["password"] = "333"
			insertUsersQuery := K3InsertQuery{
				Table:  &userTable,
				Values: insertUsersValues,
			}
			err = InsertTableFile(&insertUsersQuery)
			insertTablesValues := make([]map[string]string, 3)
			insertTablesValues[0] = make(map[string]string, 1)
			insertTablesValues[1] = make(map[string]string, 1)
			insertTablesValues[2] = make(map[string]string, 1)
			insertTablesValues[0]["table"] = K3UsersTable
			insertTablesValues[1]["table"] = K3TablesTable
			insertTablesValues[2]["table"] = K3PermissionsTable
			insertTablesQuery := K3InsertQuery{
				Table:  &tablesTable,
				Values: insertTablesValues,
			}
			err = InsertTableFile(&insertTablesQuery)
		}
		return err
	}
	return errors.New(DatabaseAlreadyExists)
}

func readAllFiles(rootDir string, callback func(path string, isDir bool) error) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return callback(path, info.IsDir())
	})
}

func StartService() error {
	err := readAllFiles(K3FilesPath, func(path string, isDir bool) error {
		if !isDir {
			if strings.HasPrefix(path, K3DataPath) && strings.HasSuffix(path, Extension) {
				path = strings.TrimPrefix(path, K3DataPath)
				path = strings.TrimSuffix(path, Extension)
				fileParts := strings.Split(path, "/")
				if len(fileParts) == 2 {
					table := &K3Table{Name: fileParts[1], Database: fileParts[0], Mu: new(sync.RWMutex)}
					err := AddFieldsTableFile(table)
					if err == nil {
						K3Tables[table.Database+"."+table.Name] = table
					} else {
						return err
					}
				}
			}
		}
		return nil
	})
	return err
}

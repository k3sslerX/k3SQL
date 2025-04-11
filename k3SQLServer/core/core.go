package core

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func CreateDatabase(name string) error {
	if !DatabaseExists(name) {
		err := CreateDatabaseFile(name)
		if err == nil {
			userTableFields := make([]string, 2)
			tablesTableFields := make([]string, 1)
			userTableFields[0], userTableFields[1] = "name", "password"
			tablesTableFields[0] = "table"
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
			queryUsersFields := make(map[string]int, 2)
			queryTablesFields := make(map[string]int, 1)
			queryTablesFields["table"] = 3
			queryUsersFields["name"] = 3
			queryUsersFields["password"] = 3
			queryUsers := K3CreateQuery{
				Table:  &userTable,
				Fields: queryUsersFields,
			}
			queryTables := K3CreateQuery{
				Table:  &tablesTable,
				Fields: queryTablesFields,
			}
			err = CreateTable(&queryUsers)
			if err == nil {
				K3Tables[userTable.Database+"."+userTable.Name] = &userTable
			}
			insertUsersValues := make([]map[string]string, 1)
			insertUsersValues[0] = make(map[string]string, 2)
			insertUsersValues[0]["name"] = "k3user"
			insertUsersValues[0]["password"] = "333"
			insertUsersQuery := K3InsertQuery{
				Table:  &userTable,
				Values: insertUsersValues,
			}
			err = InsertTable(&insertUsersQuery)
			err = CreateTable(&queryTables)
			if err == nil {
				K3Tables[userTable.Database+"."+tablesTable.Name] = &tablesTable
			}
			insertTablesValues := make([]map[string]string, 1)
			insertTablesValues[0] = make(map[string]string, 2)
			insertTablesValues[0]["table"] = K3UsersTable
			insertTablesQuery := K3InsertQuery{
				Table:  &tablesTable,
				Values: insertTablesValues,
			}
			err = InsertTable(&insertTablesQuery)
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
	K3Tables = make(map[string]*K3Table, 1)
	err := readAllFiles(K3FilesPath, func(path string, isDir bool) error {
		if !isDir {
			if strings.HasPrefix(path, K3sqlDataPath) && strings.HasSuffix(path, Extension) {
				path = strings.TrimPrefix(path, K3sqlDataPath)
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

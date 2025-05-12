package core

import (
	"errors"
	"k3SQLServer/shared"
	"k3SQLServer/storage"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	shared.K3Tables = make(map[string]*shared.K3Table, 1)
	initialised := false
	_, err := os.Stat(shared.K3FilesPath)
	if err == nil {
		initialised = true
	}
	if !initialised {
		err = os.MkdirAll(shared.K3FilesPath, os.ModePerm)
		if err == nil {
			err1 := os.MkdirAll(shared.K3DataPath, os.ModePerm)
			err2 := os.MkdirAll(shared.K3ConfigurationPath, os.ModePerm)
			if err1 == nil && err2 == nil {
				err = CreateDatabase("k3db")
			}
		}
	}
}

func CreateDatabase(name string) error {
	if !storage.DatabaseExists(name) {
		err := storage.CreateDatabaseFile(name)
		if err == nil {
			userTableFields := make([]string, 2)
			tablesTableFields := make([]string, 1)
			permissionsTableFields := make([]string, 3)
			userTableFields[0], userTableFields[1] = "name", "password"
			tablesTableFields[0] = "table"
			permissionsTableFields[0] = "user"
			permissionsTableFields[1] = "table"
			permissionsTableFields[2] = "permission"
			userTable := shared.K3Table{
				Database: name,
				Name:     shared.K3UsersTable,
				Fields:   userTableFields,
				Mu:       new(sync.RWMutex),
			}
			tablesTable := shared.K3Table{
				Database: name,
				Name:     shared.K3TablesTable,
				Fields:   tablesTableFields,
				Mu:       new(sync.RWMutex),
			}
			permissionsTable := shared.K3Table{
				Database: name,
				Name:     shared.K3PermissionsTable,
				Fields:   permissionsTableFields,
				Mu:       new(sync.RWMutex),
			}
			queryUsersFields := make(map[string]int, 2)
			queryTablesFields := make(map[string]int, 1)
			queryPermissionsFields := make(map[string]int, 3)
			queryTablesFields["table"] = shared.K3TEXT
			queryUsersFields["name"] = shared.K3TEXT
			queryUsersFields["password"] = shared.K3TEXT
			queryPermissionsFields["user"] = shared.K3TEXT
			queryPermissionsFields["table"] = shared.K3TEXT
			queryPermissionsFields["permission"] = shared.K3INT
			queryUsers := shared.K3CreateQuery{
				Table:  &userTable,
				Fields: queryUsersFields,
			}
			queryTables := shared.K3CreateQuery{
				Table:  &tablesTable,
				Fields: queryTablesFields,
			}
			queryPermissions := shared.K3CreateQuery{
				Table:  &permissionsTable,
				Fields: queryPermissionsFields,
			}
			err = storage.CreateTableFile(&queryTables)
			if err != nil {
				return err
			}
			err = storage.CreateTableFile(&queryUsers)
			if err != nil {
				return err
			}
			err = storage.CreateTableFile(&queryPermissions)
			if err != nil {
				return err
			}
			insertPermissionsValues := make([]map[string]string, 3)
			insertPermissionsValues[0] = make(map[string]string, 3)
			insertPermissionsValues[1] = make(map[string]string, 3)
			insertPermissionsValues[2] = make(map[string]string, 3)
			insertPermissionsValues[0]["user"] = "k3user"
			insertPermissionsValues[0]["table"] = shared.K3UsersTable
			insertPermissionsValues[0]["permission"] = strconv.Itoa(shared.K3Read)
			insertPermissionsValues[1]["user"] = "k3user"
			insertPermissionsValues[1]["table"] = shared.K3TablesTable
			insertPermissionsValues[1]["permission"] = strconv.Itoa(shared.K3Read)
			insertPermissionsValues[2]["user"] = "k3user"
			insertPermissionsValues[2]["table"] = shared.K3PermissionsTable
			insertPermissionsValues[2]["permission"] = strconv.Itoa(shared.K3All)
			insertPermissionsQuery := shared.K3InsertQuery{
				Table:  &permissionsTable,
				Values: insertPermissionsValues,
			}
			err = storage.InsertTableFile(&insertPermissionsQuery)
			insertUsersValues := make([]map[string]string, 1)
			insertUsersValues[0] = make(map[string]string, 2)
			insertUsersValues[0]["name"] = "k3user"
			insertUsersValues[0]["password"] = "333"
			insertUsersQuery := shared.K3InsertQuery{
				Table:  &userTable,
				Values: insertUsersValues,
			}
			err = storage.InsertTableFile(&insertUsersQuery)
			insertTablesValues := make([]map[string]string, 3)
			insertTablesValues[0] = make(map[string]string, 1)
			insertTablesValues[1] = make(map[string]string, 1)
			insertTablesValues[2] = make(map[string]string, 1)
			insertTablesValues[0]["table"] = shared.K3UsersTable
			insertTablesValues[1]["table"] = shared.K3TablesTable
			insertTablesValues[2]["table"] = shared.K3PermissionsTable
			insertTablesQuery := shared.K3InsertQuery{
				Table:  &tablesTable,
				Values: insertTablesValues,
			}
			err = storage.InsertTableFile(&insertTablesQuery)
		}
		return err
	}
	return errors.New(shared.DatabaseAlreadyExists)
}

func readAllFiles(rootDir string, callback func(path string, isDir bool) error) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return callback(path, info.IsDir())
	})
}

func uploadTables() {
	for {
		time.Sleep(time.Minute * 5)
		for k, v := range shared.K3Tables {
			if strings.HasPrefix(k, "k3_") {
				continue
			}
			if time.Since(v.LU) > 10 {
				delete(shared.K3Tables, k)
			}
		}
	}
}

func StartService() error {
	err := readAllFiles(shared.K3FilesPath, func(path string, isDir bool) error {
		if !isDir {
			if strings.HasPrefix(path, shared.K3DataPath) && strings.HasSuffix(path, shared.Extension) {
				path = strings.TrimPrefix(path, shared.K3DataPath)
				path = strings.TrimSuffix(path, shared.Extension)
				fileParts := strings.Split(path, "/")
				if len(fileParts) == 2 {
					if strings.HasPrefix(fileParts[1], "k3_") {
						table := &shared.K3Table{Name: fileParts[1], Database: fileParts[0], Mu: new(sync.RWMutex), LU: time.Now()}
						err := storage.AddFieldsTableFile(table)
						if err == nil {
							shared.K3Tables[table.Database+"."+table.Name] = table
						} else {
							return err
						}
					}
				}
			}
		}
		return nil
	})
	if err == nil {
		go uploadTables()
	}
	return err
}

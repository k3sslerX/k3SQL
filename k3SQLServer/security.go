package k3SQLServer

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func createDatabase(name string) error {
	if !databaseExists(name) {
		err := createDatabaseFile(name)
		if err == nil {
			tableFields := make([]string, 2)
			tableFields[0], tableFields[1] = "name", "password"
			table := k3Table{
				database: name,
				name:     "users",
				fields:   tableFields,
				mu:       new(sync.RWMutex),
			}
			queryFields := make(map[string]int, 2)
			queryFields["name"] = 3
			queryFields["password"] = 3
			query := k3CreateQuery{
				table:  &table,
				fields: queryFields,
			}
			err = createTable(&query)
			if err == nil {
				k3Tables[table.database+"."+table.name] = &table
			}
			insertValues := make([]map[string]string, 1)
			insertValues[0] = make(map[string]string, 2)
			insertValues[0]["name"] = "k3user"
			insertValues[0]["password"] = "333"
			insertQuery := k3InsertQuery{
				table:  &table,
				values: insertValues,
			}
			err = insertTable(&insertQuery)
		}
		return err
	}
	return errors.New(databaseAlreadyExists)
}

func readAllFiles(rootDir string, callback func(path string, isDir bool) error) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return callback(path, info.IsDir())
	})
}

func startService() error {
	k3Tables = make(map[string]*k3Table, 1)
	err := readAllFiles(k3FilesPath, func(path string, isDir bool) error {
		if !isDir {
			if strings.HasPrefix(path, k3sqlDataPath) {
				path = strings.TrimPrefix(path, k3sqlDataPath)
				path = strings.TrimSuffix(path, extension)
				fileParts := strings.Split(path, "/")
				if len(fileParts) == 2 {
					table := &k3Table{name: fileParts[1], database: fileParts[0], mu: new(sync.RWMutex)}
					err := addFieldsTableFile(table)
					if err == nil {
						k3Tables[table.database+"."+table.name] = table
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

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
			tableFields := make([]string, 2)
			tableFields[0], tableFields[1] = "name", "password"
			table := K3Table{
				Database: name,
				Name:     "users",
				Fields:   tableFields,
				Mu:       new(sync.RWMutex),
			}
			queryFields := make(map[string]int, 2)
			queryFields["name"] = 3
			queryFields["password"] = 3
			query := K3CreateQuery{
				Table:  &table,
				Fields: queryFields,
			}
			err = CreateTable(&query)
			if err == nil {
				K3Tables[table.Database+"."+table.Name] = &table
			}
			insertValues := make([]map[string]string, 1)
			insertValues[0] = make(map[string]string, 2)
			insertValues[0]["name"] = "k3user"
			insertValues[0]["password"] = "333"
			insertQuery := K3InsertQuery{
				Table:  &table,
				Values: insertValues,
			}
			err = InsertTable(&insertQuery)
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

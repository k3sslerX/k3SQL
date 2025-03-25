package k3SQLServer

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const k3configurationPath = k3FilesPath + "config/"

var k3Tables map[string]*k3Table

func readAllFiles(rootDir string, callback func(path string, isDir bool) error) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return callback(path, info.IsDir())
	})
}

func StartService() error {
	k3Tables = make(map[string]*k3Table, 1)
	err := readAllFiles(k3FilesPath, func(path string, isDir bool) error {
		if !isDir {
			if strings.HasPrefix(path, k3sqlDataPath) {
				path = strings.TrimPrefix(path, k3sqlDataPath)
				path = strings.TrimSuffix(path, extension)
				fileParts := strings.Split(path, "/")
				if len(fileParts) == 2 {
					k3Tables[fileParts[1]] = &k3Table{name: fileParts[1], database: fileParts[0], mu: new(sync.RWMutex)}
				}
			}
		}
		return nil
	})
	return err
}

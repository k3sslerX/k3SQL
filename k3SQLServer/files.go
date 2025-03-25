package k3SQLServer

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

const k3sqlDataPath = k3FilesPath + "data/"
const extension = ".k3"

const k3INT = 1
const k3FLOAT = 2
const k3TEXT = 3

func createDatabase(name string) error {
	return os.Mkdir(k3sqlDataPath+name, 0700)
}

func databaseExists(name string) bool {
	_, err := os.Stat(k3sqlDataPath + name)
	if err == nil {
		return true
	}
	return false
}

func existsTable(name string, db string) bool {
	file, err := os.Open(k3sqlDataPath + db + "/" + name + extension)
	defer file.Close()
	if err == nil {
		data := make([]byte, 128)
		_, err := file.Read(data)
		if err == nil {
			dataStr := string(data)
			parts := strings.Split(dataStr, "|")
			if len(parts) > 0 {
				return true
			}
		}
	}
	return false
}

func createTableFile(query *k3CreateQuery) error {
	file, err := os.Create(k3sqlDataPath + query.database + "/" + query.table + extension)
	defer file.Close()
	if err == nil {
		writer := bufio.NewWriter(file)
		str := ""
		for k, v := range query.fields {
			str += fmt.Sprintf("%d %s|", v, k)
		}
		_, err = writer.WriteString(strings.TrimSuffix(str, "|") + "\n")
		if err != nil {
			return err
		}
		err = writer.Flush()
		if err != nil {
			return err
		}
		k3Tables[query.table] = &k3Table{database: query.database, name: query.table, mu: new(sync.RWMutex)}
	}
	return err
}

func insertTableFile(query *k3InsertQuery) error {
	query.table.mu.Lock()
	defer query.table.mu.Unlock()
	fileRead, err := os.Open(k3sqlDataPath + query.table.database + "/" + query.table.name + extension)
	if err == nil {
		scanner := bufio.NewScanner(fileRead)
		scanner.Scan()
		dataStr := scanner.Text()
		fileRead.Close()
		parts := strings.Split(dataStr, "|")
		tableTypes := make(map[string]int, len(parts))
		for _, part := range parts {
			tableType, err := strconv.Atoi(string(part[0]))
			if err != nil {
				return err
			}
			tableTypes[part[2:]] = tableType
		}
		file, err := os.OpenFile(k3sqlDataPath+query.table.database+"/"+query.table.name+extension, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer file.Close()
		for _, value := range query.values {
			str := ""
			for k, _ := range tableTypes {
				if tableTypes[k] == k3INT {
					v, ok := value[k]
					if ok {
						_, err := strconv.Atoi(v)
						if err != nil {
							return err
						}
						str += v + "|"
					} else {
						return errors.New(fmt.Sprintf("empty column: %s", k))
					}
				} else if tableTypes[k] == k3FLOAT {
					v, ok := value[k]
					if ok {
						_, err := strconv.ParseFloat(v, 64)
						if err != nil {
							return err
						}
						str += v + "|"
					} else {
						return errors.New(fmt.Sprintf("empty column: %s", k))
					}
				} else if tableTypes[k] == k3TEXT {
					v, ok := value[k]
					if ok {
						str += v + "|"
					} else {
						return errors.New(fmt.Sprintf("empty column: %s", k))
					}
				} else {
					return errors.New("unknown type")
				}
			}
			_, err = file.WriteString(strings.TrimSuffix(str, "|") + "\n")
			if err != nil {
				return err
			}
		}
	}
	return err
}

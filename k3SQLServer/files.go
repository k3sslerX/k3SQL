package k3SQLServer

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const K3sqlDataPath = k3FilesPath + "data/"
const extension = ".k3"

const K3INT = 1
const K3FLOAT = 2
const K3TEXT = 3

func CreateDatabase(name string) error {
	return os.Mkdir(K3sqlDataPath+name, 0700)
}

func databaseExists(name string) bool {
	_, err := os.Stat(K3sqlDataPath + name)
	if err == nil {
		return true
	}
	return false
}

func existsTable(name string, db string) bool {
	file, err := os.Open(K3sqlDataPath + db + "/" + name + extension)
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

func createTableFile(query *K3CreateQuery) error {
	file, err := os.Create(K3sqlDataPath + query.Database + "/" + query.Table + extension)
	defer file.Close()
	if err == nil {
		writer := bufio.NewWriter(file)
		str := ""
		for k, v := range query.Fields {
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
	}
	return err
}

func insertTableFile(query *K3InsertQuery) error {
	fileRead, err := os.Open(K3sqlDataPath + query.Database + "/" + query.Table + extension)
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
		file, err := os.OpenFile(K3sqlDataPath+query.Database+"/"+query.Table+extension, os.O_APPEND|os.O_WRONLY, 0644)
		defer file.Close()
		for _, value := range query.Values {
			str := ""
			for k, _ := range tableTypes {
				if tableTypes[k] == K3INT {
					v := value[k]
					_, err := strconv.Atoi(v)
					if err != nil {
						return err
					}
					str += v + "|"
				} else if tableTypes[k] == K3FLOAT {
					v := value[k]
					_, err := strconv.ParseFloat(v, 64)
					if err != nil {
						return err
					}
					str += v + "|"
				} else if tableTypes[k] == K3TEXT {
					v := value[k]
					str += v + "|"
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

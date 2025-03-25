package k3SQLServer

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

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

func existsTable(table *k3Table) bool {
	file, err := os.Open(k3sqlDataPath + table.database + "/" + table.name + extension)
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
	file, err := os.Create(k3sqlDataPath + query.table.database + "/" + query.table.name + extension)
	defer file.Close()
	if err == nil {
		writer := bufio.NewWriter(file)
		str := ""
		for _, field := range query.table.fields {
			str += fmt.Sprintf("%d %s|", query.fields[field], field)
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

func selectTableFile(query *k3SelectQuery) ([]map[string]string, error) {
	query.table.mu.RLock()
	defer query.table.mu.RUnlock()
	fileRead, err := os.Open(k3sqlDataPath + query.table.database + "/" + query.table.name + extension)
	defer fileRead.Close()
	if err == nil {
		scanner := bufio.NewScanner(fileRead)
		scanner.Scan()
		dataStr := scanner.Text()
		parts := strings.Split(dataStr, "|")
		tableTypes := make(map[string]int, len(parts))
		tableFields := make([]string, 0)
		for _, part := range parts {
			tableType, err := strconv.Atoi(string(part[0]))
			if err != nil {
				return nil, err
			}
			tableTypes[part[2:]] = tableType
			tableFields = append(tableFields, part[2:])
		}
		values := query.values
		if len(values) > 0 {
			if values[0] == "*" && len(values) == 1 {
				values = values[1:]
				for k, _ := range tableTypes {
					values = append(values, k)
				}
			} else if values[0] == "*" {
				return nil, errors.New(invalidSQLSyntax)
			} else {
				for _, value := range values {
					flag := false
					for k, _ := range tableTypes {
						if value == k {
							flag = true
							break
						}
					}
					if flag == false {
						return nil, errors.New(fmt.Sprintf("invalid column: %s", value))
					}
				}
			}
			lines := make([]map[string]string, 0)
			for scanner.Scan() {
				lineParts := strings.Split(scanner.Text(), "|")
				tmpMap := make(map[string]string, len(lineParts))
				valuesCnt := 0
				for i := 0; i < len(lineParts) && valuesCnt < len(values); i++ {
					if tableFields[i] == values[valuesCnt] {
						tmpMap[tableFields[i]] = lineParts[i]
						valuesCnt++
					}
				}
				lines = append(lines, tmpMap)
			}
			return lines, nil
		} else {
			return nil, errors.New(invalidSQLSyntax)
		}
	}
	return nil, err
}

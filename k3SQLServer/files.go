package k3SQLServer

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func createDatabaseFile(name string) error {
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

func addFieldsTableFile(table *k3Table) error {
	file, err := os.Open(k3sqlDataPath + table.database + "/" + table.name + extension)
	if err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Scan()
		dataStr := scanner.Text()
		parts := strings.Split(dataStr, "|")
		tableFields := make([]string, len(parts))
		for i := 0; i < len(parts); i++ {
			tableFields[i] = parts[i][2:]
		}
		table.fields = tableFields
	}
	return err
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
			for _, k := range query.table.fields {
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

func dropTableFile(table *k3Table) error {
	return os.Remove(k3sqlDataPath + table.database + "/" + table.name + extension)
}

func selectTableFile(query *k3SelectQuery) ([]map[string]string, error) {
	query.table.mu.RLock()
	defer query.table.mu.RUnlock()
	fileRead, err := os.Open(k3sqlDataPath + query.table.database + "/" + query.table.name + extension)
	if err != nil {
		return nil, err
	}
	defer fileRead.Close()
	scanner := bufio.NewScanner(fileRead)
	scanner.Scan()
	var results []map[string]string
	for scanner.Scan() {
		line := scanner.Text()
		record := parseRecord(line, query.table.fields)

		if satisfiesConditions(record, query.conditions) {
			filteredRecord := make(map[string]string)
			for _, field := range query.values {
				if field == "*" {
					for k, v := range record {
						filteredRecord[k] = v
					}
					break
				} else {
					if val, ok := record[field]; ok {
						filteredRecord[field] = val
					} else {
						return nil, fmt.Errorf("field %s not found", field)
					}
				}
			}
			results = append(results, filteredRecord)
		}
	}

	return results, nil
}

func parseRecord(line string, fields []string) map[string]string {
	values := strings.Split(line, "|")
	record := make(map[string]string)
	for i, field := range fields {
		if i < len(values) {
			record[field] = values[i]
		}
	}
	return record
}

func satisfiesConditions(record map[string]string, conditions []Condition) bool {
	for _, cond := range conditions {
		recordValue, ok := record[cond.Column]
		if !ok {
			return false
		}

		switch cond.Operator {
		case "LIKE":
			likePattern := strings.ReplaceAll(cond.Value, "%", ".*")
			likePattern = strings.ReplaceAll(likePattern, "_", ".")
			likePattern = "^" + likePattern + "$"

			matched, err := regexp.MatchString(likePattern, recordValue)
			if err != nil || !matched {
				return false
			}
		case "=":
			if recordValue != cond.Value {
				return false
			}
		case "!=":
			if recordValue == cond.Value {
				return false
			}
		case ">":
			if !compareValues(recordValue, cond.Value, false) {
				return false
			}
		case "<":
			if !compareValues(cond.Value, recordValue, false) {
				return false
			}
		case ">=":
			if !compareValues(recordValue, cond.Value, true) {
				return false
			}
		case "<=":
			if !compareValues(cond.Value, recordValue, true) {
				return false
			}
		}
	}
	return true
}

func compareValues(a, b string, allowEqual bool) bool {
	numA, errA := strconv.ParseFloat(a, 64)
	numB, errB := strconv.ParseFloat(b, 64)

	if errA == nil && errB == nil {
		if allowEqual && numA == numB {
			return true
		}
		return numA > numB
	}

	if allowEqual && a == b {
		return true
	}
	return a > b
}

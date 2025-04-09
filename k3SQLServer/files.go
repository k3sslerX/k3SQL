package k3SQLServer

import (
	"bufio"
	"errors"
	"fmt"
	"golang.org/x/crypto/bcrypt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func createDatabaseFile(name string) error {
	return os.Mkdir(k3sqlDataPath+name, 0700)
}

func databaseExists(name string) bool {
	if len(name) > 0 {
		_, err := os.Stat(k3sqlDataPath + name)
		if err == nil {
			return true
		}
	}
	return false
}

func checkCredentialsFiles(dbName, user, password string) (bool, error) {
	if !databaseExists(dbName) {
		return false, errors.New(databaseNotExists)
	}

	tableKey := dbName + ".users"
	usersTable, ok := k3Tables[tableKey]
	if !ok {
		return false, errors.New(tableNotExists)
	}

	usersTable.mu.RLock()
	defer usersTable.mu.RUnlock()

	filePath := k3sqlDataPath + dbName + "/users" + extension
	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	if !scanner.Scan() {
		return false, errors.New(fileFormatError)
	}

	for scanner.Scan() {
		record := parseUserRecord(scanner.Text())
		if record["name"] == user {
			err = bcrypt.CompareHashAndPassword([]byte(password), []byte(record["password"]))
			if err == nil {
				return true, nil
			} else {
				return false, errors.New(wrongPassword)
			}
		}
	}

	return false, errors.New(userNotFound)
}

func parseUserRecord(line string) map[string]string {
	parts := strings.Split(line, "|")
	return map[string]string{
		"name":     parts[0],
		"password": parts[1],
	}
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
	table.mu.Lock()
	defer table.mu.Unlock()
	return os.Remove(k3sqlDataPath + table.database + "/" + table.name + extension)
}

func selectTableFile(query *k3SelectQuery) ([]map[string]string, int, error) {
	query.table.mu.RLock()
	defer query.table.mu.RUnlock()
	fileRead, err := os.Open(k3sqlDataPath + query.table.database + "/" + query.table.name + extension)
	if err != nil {
		return nil, 0, err
	}
	defer fileRead.Close()
	scanner := bufio.NewScanner(fileRead)
	scanner.Scan()
	var results []map[string]string
	rows := 0
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
						return nil, 0, fmt.Errorf("field %s not found", field)
					}
				}
			}
			results = append(results, filteredRecord)
			rows++
		}
	}
	return results, rows, nil
}

func updateTableFile(query *k3UpdateQuery) (int, error) {
	query.table.mu.Lock()
	defer query.table.mu.Unlock()

	filePath := k3sqlDataPath + query.table.database + "/" + query.table.name + extension
	tempFilePath := filePath + ".tmp"

	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return 0, err
	}
	defer tempFile.Close()

	scanner := bufio.NewScanner(file)
	writer := bufio.NewWriter(tempFile)

	if !scanner.Scan() {
		return 0, scanner.Err()
	}
	if _, err := writer.WriteString(scanner.Text() + "\n"); err != nil {
		return 0, err
	}

	updatedCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		record := parseRecord(line, query.table.fields)
		if len(query.conditions) == 0 || satisfiesConditions(record, query.conditions) {
			for col, val := range query.setValues {
				if _, exists := record[col]; exists {
					record[col] = val
				}
			}
			updatedCount++
			var newLine strings.Builder
			for i, field := range query.table.fields {
				if i > 0 {
					newLine.WriteString("|")
				}
				newLine.WriteString(record[field])
			}
			line = newLine.String()
		}
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return updatedCount, err
		}
	}
	if err := scanner.Err(); err != nil {
		return updatedCount, err
	}
	if err := writer.Flush(); err != nil {
		return updatedCount, err
	}
	if err := os.Rename(tempFilePath, filePath); err != nil {
		return updatedCount, err
	}

	return updatedCount, nil
}

func deleteTableFile(query *k3DeleteQuery) (int, error) {
	query.table.mu.Lock()
	defer query.table.mu.Unlock()

	filePath := k3sqlDataPath + query.table.database + "/" + query.table.name + extension
	tempFilePath := filePath + ".tmp"

	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return 0, err
	}
	defer tempFile.Close()
	scanner := bufio.NewScanner(file)
	writer := bufio.NewWriter(tempFile)
	if !scanner.Scan() {
		return 0, scanner.Err()
	}
	if _, err := writer.WriteString(scanner.Text() + "\n"); err != nil {
		return 0, err
	}
	deletedCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		record := parseRecord(line, query.table.fields)
		if len(query.conditions) == 0 || !satisfiesConditions(record, query.conditions) {
			if _, err := writer.WriteString(line + "\n"); err != nil {
				return deletedCount, err
			}
		} else {
			deletedCount++
		}
	}
	if err := scanner.Err(); err != nil {
		return deletedCount, err
	}
	if err := writer.Flush(); err != nil {
		return deletedCount, err
	}
	if err := os.Rename(tempFilePath, filePath); err != nil {
		return deletedCount, err
	}

	return deletedCount, nil
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

func satisfiesConditions(record map[string]string, conditions []k3Condition) bool {
	for _, cond := range conditions {
		recordValue, ok := record[cond.column]
		if !ok {
			return false
		}

		switch cond.operator {
		case "LIKE":
			likePattern := strings.ReplaceAll(cond.value, "%", ".*")
			likePattern = strings.ReplaceAll(likePattern, "_", ".")
			likePattern = "^" + likePattern + "$"

			matched, err := regexp.MatchString(likePattern, recordValue)
			if err != nil || !matched {
				return false
			}
		case "=":
			if recordValue != cond.value {
				return false
			}
		case "!=":
			if recordValue == cond.value {
				return false
			}
		case ">":
			if !compareValues(recordValue, cond.value, false) {
				return false
			}
		case "<":
			if !compareValues(cond.value, recordValue, false) {
				return false
			}
		case ">=":
			if !compareValues(recordValue, cond.value, true) {
				return false
			}
		case "<=":
			if !compareValues(cond.value, recordValue, true) {
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

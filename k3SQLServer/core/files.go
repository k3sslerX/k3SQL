package core

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

func CreateDatabaseFile(name string) error {
	return os.Mkdir(K3sqlDataPath+name, 0700)
}

func DatabaseExists(name string) bool {
	if len(name) > 0 {
		_, err := os.Stat(K3sqlDataPath + name)
		if err == nil {
			return true
		}
	}
	return false
}

func CheckCredentialsFiles(dbName, user, password string) (bool, error) {
	if !DatabaseExists(dbName) {
		return false, errors.New(DatabaseNotExists)
	}

	TableKey := dbName + ".users"
	usersTable, ok := K3Tables[TableKey]
	if !ok {
		return false, errors.New(TableNotExists)
	}

	usersTable.Mu.RLock()
	defer usersTable.Mu.RUnlock()

	filePath := K3sqlDataPath + dbName + "/users" + Extension
	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	if !scanner.Scan() {
		return false, errors.New(FileFormatError)
	}

	for scanner.Scan() {
		record := parseUserRecord(scanner.Text())
		if record["name"] == user {
			err = bcrypt.CompareHashAndPassword([]byte(password), []byte(record["password"]))
			if err == nil {
				return true, nil
			} else {
				return false, errors.New(WrongPassword)
			}
		}
	}

	return false, errors.New(UserNotFound)
}

func parseUserRecord(line string) map[string]string {
	parts := strings.Split(line, "|")
	return map[string]string{
		"name":     parts[0],
		"password": parts[1],
	}
}

func ExistsTable(Table *K3Table) bool {
	file, err := os.Open(K3sqlDataPath + Table.Database + "/" + Table.Name + Extension)
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

func AddFieldsTableFile(Table *K3Table) error {
	file, err := os.Open(K3sqlDataPath + Table.Database + "/" + Table.Name + Extension)
	if err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Scan()
		dataStr := scanner.Text()
		parts := strings.Split(dataStr, "|")
		TableFields := make([]string, len(parts))
		for i := 0; i < len(parts); i++ {
			TableFields[i] = parts[i][2:]
		}
		Table.Fields = TableFields
	}
	return err
}

func CreateTableFile(query *K3CreateQuery) error {
	file, err := os.Create(K3sqlDataPath + query.Table.Database + "/" + query.Table.Name + Extension)
	defer file.Close()
	if err == nil {
		writer := bufio.NewWriter(file)
		str := ""
		for _, field := range query.Table.Fields {
			str += fmt.Sprintf("%d %s|", query.Fields[field], field)
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

func InsertTableFile(query *K3InsertQuery) error {
	query.Table.Mu.Lock()
	defer query.Table.Mu.Unlock()
	fileRead, err := os.Open(K3sqlDataPath + query.Table.Database + "/" + query.Table.Name + Extension)
	if err == nil {
		scanner := bufio.NewScanner(fileRead)
		scanner.Scan()
		dataStr := scanner.Text()
		fileRead.Close()
		parts := strings.Split(dataStr, "|")
		TableTypes := make(map[string]int, len(parts))
		for _, part := range parts {
			TableType, err := strconv.Atoi(string(part[0]))
			if err != nil {
				return err
			}
			TableTypes[part[2:]] = TableType
		}
		file, err := os.OpenFile(K3sqlDataPath+query.Table.Database+"/"+query.Table.Name+Extension, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer file.Close()
		for _, value := range query.Values {
			str := ""
			for _, k := range query.Table.Fields {
				if TableTypes[k] == K3INT {
					v, ok := value[k]
					if ok {
						_, err := strconv.Atoi(v)
						if err != nil {
							return err
						}
						str += v + "|"
					} else {
						return errors.New(fmt.Sprintf("empty Column: %s", k))
					}
				} else if TableTypes[k] == K3FLOAT {
					v, ok := value[k]
					if ok {
						_, err := strconv.ParseFloat(v, 64)
						if err != nil {
							return err
						}
						str += v + "|"
					} else {
						return errors.New(fmt.Sprintf("empty Column: %s", k))
					}
				} else if TableTypes[k] == K3TEXT {
					v, ok := value[k]
					if ok {
						str += v + "|"
					} else {
						return errors.New(fmt.Sprintf("empty Column: %s", k))
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

func DropTableFile(Table *K3Table) error {
	Table.Mu.Lock()
	defer Table.Mu.Unlock()
	return os.Remove(K3sqlDataPath + Table.Database + "/" + Table.Name + Extension)
}

func SelectTableFile(query *K3SelectQuery) ([]map[string]string, int, error) {
	query.Table.Mu.RLock()
	defer query.Table.Mu.RUnlock()
	fileRead, err := os.Open(K3sqlDataPath + query.Table.Database + "/" + query.Table.Name + Extension)
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
		record := parseRecord(line, query.Table.Fields)

		if satisfiesConditions(record, query.Conditions) {
			filteredRecord := make(map[string]string)
			for _, field := range query.Values {
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

func UpdateTableFile(query *K3UpdateQuery) (int, error) {
	query.Table.Mu.Lock()
	defer query.Table.Mu.Unlock()

	filePath := K3sqlDataPath + query.Table.Database + "/" + query.Table.Name + Extension
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
		record := parseRecord(line, query.Table.Fields)
		if len(query.Conditions) == 0 || satisfiesConditions(record, query.Conditions) {
			for col, val := range query.SetValues {
				if _, exists := record[col]; exists {
					record[col] = val
				}
			}
			updatedCount++
			var newLine strings.Builder
			for i, field := range query.Table.Fields {
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

func DeleteTableFile(query *K3DeleteQuery) (int, error) {
	query.Table.Mu.Lock()
	defer query.Table.Mu.Unlock()

	filePath := K3sqlDataPath + query.Table.Database + "/" + query.Table.Name + Extension
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
		record := parseRecord(line, query.Table.Fields)
		if len(query.Conditions) == 0 || !satisfiesConditions(record, query.Conditions) {
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

func satisfiesConditions(record map[string]string, conditions []K3Condition) bool {
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

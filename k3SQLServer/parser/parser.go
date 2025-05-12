package parser

import (
	"errors"
	"fmt"
	"k3SQLServer/shared"
	"k3SQLServer/storage"
	"os"
	"strings"
	"sync"
)

func ParseUserQuery(queryStr, db string) (*shared.K3UserQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(shared.K3UserQuery)
	query.Database = db
	newFlag := false
	delFlag := false
	pwdFlag := false
	for _, part := range parts {
		if strings.EqualFold(part, "new") {
			newFlag = true
			continue
		}
		if strings.EqualFold(part, "delete") {
			delFlag = true
			continue
		}
		if newFlag {
			query.Username = part
			query.Action = shared.K3CREATE
			newFlag = false
			pwdFlag = true
			continue
		}
		if delFlag {
			query.Username = part
			query.Action = shared.K3DELETE
			delFlag = false
			pwdFlag = true
			continue
		}
		if pwdFlag {
			query.Password = part
			pwdFlag = false
			continue
		}
	}
	return query, nil
}

func ParseCreateQuery(queryStr, db string) (*shared.K3CreateQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(shared.K3CreateQuery)
	tableFlag := false
	ifFlag := false
	notFlag := false
	databaseFlag := false
	for _, part := range parts {
		if strings.EqualFold(part, "database") {
			databaseFlag = true
			continue
		}
		if strings.EqualFold(part, "table") {
			tableFlag = true
			continue
		} else if strings.EqualFold(part, "if") {
			ifFlag = true
			continue
		} else if strings.EqualFold(part, "not") {
			if !ifFlag {
				return nil, errors.New(shared.InvalidSQLSyntax)
			}
			notFlag = true
			continue
		} else if strings.EqualFold(part, "exists") {
			if !ifFlag {
				return nil, errors.New(shared.InvalidSQLSyntax)
			}
			if !notFlag {
				return nil, errors.New(shared.InvalidSQLLogic)
			}
			continue
		}
		if tableFlag {
			table := shared.K3Table{Name: part, Database: db, Mu: new(sync.RWMutex)}
			query.Table = &table
			tableFlag = false
		}
		if databaseFlag {
			table := shared.K3Table{Name: "", Database: part, Mu: nil}
			query.Table = &table
			return query, nil
		}
	}
	fieldsStr := ""
	flag := false
	for i := 0; i < len(queryStr); i++ {
		if queryStr[i] == '(' {
			flag = true
			continue
		}
		if queryStr[i] == ')' {
			break
		}
		if flag {
			fieldsStr += string(queryStr[i])
		}
	}
	fieldsPartsTypes := strings.Split(fieldsStr, ",")
	fields := make(map[string]int, len(fieldsPartsTypes))
	queryFields := make([]string, len(fieldsPartsTypes))
	for i := 0; i < len(fieldsPartsTypes); i++ {
		fieldsParts := strings.Fields(fieldsPartsTypes[i])
		if len(fieldsParts) != 2 {
			return nil, errors.New(shared.InvalidSQLSyntax)
		}
		switch strings.ToUpper(fieldsParts[1]) {
		case "INT":
			fields[fieldsParts[0]] = shared.K3INT
		case "FLOAT":
			fields[fieldsParts[0]] = shared.K3FLOAT
		case "TEXT":
			fields[fieldsParts[0]] = shared.K3TEXT
		default:
			return nil, errors.New(fmt.Sprintf("Invalid type: %s", fieldsParts[i+1]))
		}
		queryFields[i] = fieldsParts[0]
	}
	query.Fields = fields
	query.Table.Fields = queryFields
	return query, nil
}

func ParseInsertQuery(queryStr, db string) (*shared.K3InsertQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(shared.K3InsertQuery)
	intoFlag := false
	ValuesFlag := false
	fieldsFlag := false
	ValuesStr := ""
	fieldsStr := ""
	for _, part := range parts {
		if strings.EqualFold(part, "into") {
			intoFlag = true
			continue
		} else if strings.EqualFold(part, "values") {
			fieldsFlag = false
			ValuesFlag = true
			continue
		}
		if intoFlag {
			table, ok := shared.K3Tables[db+"."+part]
			if ok {
				query.Table = table
			} else {
				return nil, errors.New(shared.TableNotExists)
			}
			intoFlag = false
			fieldsFlag = true
			continue
		}
		if ValuesFlag {
			ValuesStr += part
		}
		if fieldsFlag {
			fieldsStr += part
		}
	}
	fieldsStr = strings.TrimSuffix(fieldsStr, ")")
	fieldsStr = strings.TrimPrefix(fieldsStr, "(")
	fieldsSlice := strings.Split(fieldsStr, ",")
	ValuesSlice := make([]string, strings.Count(ValuesStr, "("))
	str := ""
	cnt := 0
	flag := false
	for _, el := range ValuesStr {
		if el == '(' {
			flag = true
			continue
		}
		if el == ')' {
			ValuesSlice[cnt] = str
			str = ""
			cnt++
			flag = false
			continue
		}
		if flag {
			str += string(el)
		}
	}
	tmpMap := make([]map[string]string, len(ValuesSlice))
	cnt = 0
	for _, value := range ValuesSlice {
		tmpMap[cnt] = make(map[string]string, len(fieldsSlice))
		valueParts := strings.Split(value, ",")
		if len(fieldsSlice) != len(valueParts) {
			return nil, errors.New(shared.InvalidSQLSyntax)
		}
		for i := 0; i < len(fieldsSlice); i++ {
			tmpMap[cnt][fieldsSlice[i]] = valueParts[i]
		}
		cnt++
	}
	query.Values = tmpMap
	return query, nil
}

func ParseUpdateQuery(queryStr, db string) (*shared.K3UpdateQuery, error) {
	parts := strings.Fields(queryStr)
	query := &shared.K3UpdateQuery{
		SetValues:  make(map[string]string),
		Conditions: make([]shared.K3Condition, 0),
	}

	updateFlag := true
	setFlag := false
	whereFlag := false
	var whereParts []string
	var setParts []string

	for i := 0; i < len(parts); i++ {
		part := strings.TrimSuffix(parts[i], ",")
		upperPart := strings.ToUpper(part)

		switch upperPart {
		case "UPDATE":
			continue
		case "SET":
			updateFlag = false
			setFlag = true
			whereFlag = false
		case "WHERE":
			setFlag = false
			whereFlag = true
			updateFlag = false
		default:
			if updateFlag {
				table, ok := shared.K3Tables[db+"."+part]
				if !ok {
					filePath := shared.K3DataPath + db + "/" + part + shared.Extension
					_, err := os.Open(filePath)
					if err == nil {
						tableFile := &shared.K3Table{Name: part, Database: db, Mu: new(sync.RWMutex)}
						err = storage.AddFieldsTableFile(tableFile)
						if err == nil {
							shared.K3Tables[tableFile.Database+"."+tableFile.Name] = tableFile
							table = tableFile
						} else {
							return nil, err
						}
					} else {
						return nil, errors.New(shared.TableNotExists)
					}
				}
				query.Table = table
			} else if setFlag {
				setParts = append(setParts, part)
			} else if whereFlag {
				whereParts = append(whereParts, part)
			}
		}
	}
	setClause := strings.Join(setParts, " ")
	setPairs := strings.Split(setClause, ",")
	for _, pair := range setPairs {
		pair = strings.TrimSpace(pair)
		if eqIdx := strings.Index(pair, "="); eqIdx > 0 {
			column := strings.TrimSpace(pair[:eqIdx])
			value := strings.TrimSpace(pair[eqIdx+1:])
			if len(value) > 0 && (value[0] == '\'' || value[0] == '"') {
				value = strings.Trim(value, "'\"")
			}
			query.SetValues[column] = value
		}
	}
	if len(whereParts) > 0 {
		conditions, err := parseWhereClause(strings.Join(whereParts, " "))
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	return query, nil
}

func ParseDropQuery(queryStr, db string) (*shared.K3Table, error) {
	parts := strings.Fields(queryStr)
	tableFlag := false
	ifFlag := false
	for _, part := range parts {
		if strings.EqualFold(part, "drop") {
			continue
		} else if strings.EqualFold(part, "table") {
			tableFlag = true
			continue
		} else if strings.EqualFold(part, "if") {
			if tableFlag {
				ifFlag = true
				tableFlag = false
			} else {
				return nil, errors.New(shared.InvalidSQLSyntax)
			}
			continue
		} else if strings.EqualFold(part, "not") {
			if ifFlag {
				return nil, errors.New(shared.InvalidSQLLogic)
			}
			continue
		} else if strings.EqualFold(part, "exists") {
			if ifFlag {
				ifFlag = false
				tableFlag = true
			}
			continue
		}
		if tableFlag {
			table, ok := shared.K3Tables[db+"."+part]
			if !ok {
				filePath := shared.K3DataPath + db + "/" + part + shared.Extension
				_, err := os.Open(filePath)
				if err == nil {
					tableFile := &shared.K3Table{Name: part, Database: db, Mu: new(sync.RWMutex)}
					err = storage.AddFieldsTableFile(tableFile)
					if err == nil {
						shared.K3Tables[tableFile.Database+"."+tableFile.Name] = tableFile
						table = tableFile
					} else {
						return nil, err
					}
				} else {
					return nil, errors.New(shared.TableNotExists)
				}
			}
			return table, nil
		}
	}
	return nil, errors.New(shared.InvalidSQLSyntax)
}

func ParseSelectQuery(queryStr, db string) (*shared.K3SelectQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(shared.K3SelectQuery)
	query.Values = make([]string, 0)
	query.Conditions = make([]shared.K3Condition, 0)

	selectCond := false
	fromCond := false
	whereCond := false
	var whereParts []string

	for _, part := range parts {
		part = strings.TrimSuffix(part, ",")
		upperPart := strings.ToUpper(part)

		switch upperPart {
		case "SELECT":
			selectCond = true
			fromCond = false
			whereCond = false
		case "FROM":
			fromCond = true
			selectCond = false
			whereCond = false
		case "WHERE":
			whereCond = true
			fromCond = false
			selectCond = false
		default:
			if selectCond {
				query.Values = append(query.Values, part)
			} else if fromCond {
				table, ok := shared.K3Tables[db+"."+part]
				if !ok {
					filePath := shared.K3DataPath + db + "/" + part + shared.Extension
					_, err := os.Open(filePath)
					if err == nil {
						tableFile := &shared.K3Table{Name: part, Database: db, Mu: new(sync.RWMutex)}
						err = storage.AddFieldsTableFile(tableFile)
						if err == nil {
							shared.K3Tables[tableFile.Database+"."+tableFile.Name] = tableFile
							table = tableFile
						} else {
							return nil, err
						}
					} else {
						return nil, errors.New(shared.TableNotExists)
					}
				}
				query.Table = table
			} else if whereCond {
				whereParts = append(whereParts, part)
			}
		}
	}
	if len(whereParts) > 0 {
		conditions, err := parseWhereClause(strings.Join(whereParts, " "))
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	return query, nil
}

func ParseDeleteQuery(queryStr, db string) (*shared.K3DeleteQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(shared.K3DeleteQuery)
	query.Conditions = make([]shared.K3Condition, 0)

	fromFlag := false
	whereFlag := false
	var whereParts []string

	for _, part := range parts {
		part = strings.TrimSuffix(part, ",")
		upperPart := strings.ToUpper(part)

		switch upperPart {
		case "DELETE":
			continue
		case "FROM":
			fromFlag = true
			whereFlag = false
		case "WHERE":
			whereFlag = true
			fromFlag = false
		default:
			if fromFlag {
				table, ok := shared.K3Tables[db+"."+part]
				if !ok {
					return nil, errors.New(shared.TableNotExists)
				}
				query.Table = table
				fromFlag = false
			} else if whereFlag {
				whereParts = append(whereParts, part)
			}
		}
	}

	if len(whereParts) > 0 {
		conditions, err := parseWhereClause(strings.Join(whereParts, " "))
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	return query, nil
}

func parseWhereClause(whereClause string) ([]shared.K3Condition, error) {
	var conditions []shared.K3Condition
	andParts := strings.Split(whereClause, "and")
	for _, part := range andParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		cond, err := parseSingleCondition(part)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}

	return conditions, nil
}

func parseSingleCondition(condStr string) (shared.K3Condition, error) {
	if likeIdx := strings.Index(strings.ToUpper(condStr), "LIKE "); likeIdx >= 0 {
		column := strings.TrimSpace(condStr[:likeIdx])
		value := strings.TrimSpace(condStr[likeIdx+5:])

		if len(value) > 0 && (value[0] == '\'' || value[0] == '"') {
			value = strings.Trim(value, "'\"")
		}

		return shared.K3Condition{
			Column:   column,
			Operator: "LIKE",
			Value:    value,
		}, nil
	}

	operators := []string{">=", "<=", "=", "!=", ">", "<"}

	for _, op := range operators {
		if opIdx := strings.Index(condStr, op); opIdx >= 0 {
			column := strings.TrimSpace(condStr[:opIdx])
			value := strings.TrimSpace(condStr[opIdx+len(op):])

			if len(value) > 0 && (value[0] == '\'' || value[0] == '"') {
				value = strings.Trim(value, "'\"")
			}

			return shared.K3Condition{
				Column:   column,
				Operator: op,
				Value:    value,
			}, nil
		}
	}

	return shared.K3Condition{}, errors.New(shared.InvalidSQLSyntax)
}

package k3SQLServer

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

func checkQuery(queryStr string) bool {
	parts := strings.Fields(queryStr)
	if len(parts) > 0 {
		part := parts[0]
		switch strings.ToLower(part) {
		case "select":
			return checkSelectQuery(queryStr)
		case "create":
			return checkCreateQuery(queryStr)
		case "drop":
			return checkDropQuery(queryStr)
		case "insert":
			return checkInsertQuery(queryStr)
		case "update":
			return checkUpdateQuery(queryStr)
		case "alter":
			return checkAlterQuery(queryStr)
		case "explain":
			return checkQuery(queryStr[len(part):])
		default:
			return false
		}
	}
	return false
}

func checkSelectQuery(query string) bool {
	selectRegex := regexp.MustCompile(`(?is)^\s*SELECT\s+(?:(?:DISTINCT|ALL)\s+)?(?:[\w*]+(?:\s*,\s*[\w*]+)*|\*)\s+FROM\s+\w+(?:\s+(?:AS\s+)?\w+)?(?:\s+(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+\w+(?:\s+(?:AS\s+)?\w+)?\s+ON\s+[^;]+)?(?:\s+WHERE\s+[^;]+)?(?:\s+GROUP\s+BY\s+[^;]+)?(?:\s+HAVING\s+[^;]+)?(?:\s+ORDER\s+BY\s+[^;]+)?(?:\s+(?:LIMIT\s+\d+(?:\s*,\s*\d+|\s+OFFSET\s+\d+)?)?)?\s*;?\s*$`)
	return selectRegex.MatchString(query)
}

func checkCreateQuery(query string) bool {
	createRegex := regexp.MustCompile(`(?i)^\s*CREATE\s+(TEMPORARY\s+)?(TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([` + "`" + `"]?\w+[` + "`" + `"]?\.)?[` + "`" + `"]?\w+[` + "`" + `"]?\s*\(.*\)|(DATABASE|SCHEMA)\s+(IF\s+NOT\s+EXISTS\s+)?[` + "`" + `"]?\w+[` + "`" + `"]?)\s*(;)?\s*$`)
	return createRegex.MatchString(query)
}

func checkDropQuery(query string) bool {
	dropRegex := regexp.MustCompile(`(?i)^\s*DROP\s+(TABLE|DATABASE|SCHEMA|INDEX|VIEW|TRIGGER|PROCEDURE|FUNCTION)\s+(IF\s+EXISTS\s+)?([` + "`" + `"]?\w+[` + "`" + `"]?\.)?[` + "`" + `"]?\w+[` + "`" + `"]?\s*(;)?\s*$`)
	return dropRegex.MatchString(query)
}

func checkInsertQuery(query string) bool {
	insertRegex := regexp.MustCompile(`(?is)^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+\w+\s*\(\s*\w+(?:\s*,\s*\w+)*\s*\)\s*VALUES\s*\([^)]+\)(?:\s*,\s*\([^)]+\))*\s*;?\s*$`)
	return insertRegex.MatchString(query)
}

func checkUpdateQuery(query string) bool {
	updateRegex := regexp.MustCompile(`(?is)^\s*UPDATE\s+(?:\w+\s+(?:AS\s+)?\w+\s*,\s*)*\w+\s+(?:AS\s+)?\w*\s+SET\s+\w+\s*=\s*(?:'[^']*'|"[^"]*"|\d+\.?\d*|\w+)(?:\s*,\s*\w+\s*=\s*(?:'[^']*'|"[^"]*"|\d+\.?\d*|\w+))*\s+(?:WHERE\s+.+?)?\s*;?\s*$`)
	return updateRegex.MatchString(query)
}

func checkAlterQuery(query string) bool {
	return true
}

func parseCreateQuery(queryStr, db string) (*k3CreateQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(k3CreateQuery)
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
				return nil, errors.New(invalidSQLSyntax)
			}
			notFlag = true
			continue
		} else if strings.EqualFold(part, "exists") {
			if !ifFlag {
				return nil, errors.New(invalidSQLSyntax)
			}
			if !notFlag {
				return nil, errors.New(invalidSQLLogic)
			}
			continue
		}
		if tableFlag {
			table := k3Table{name: part, database: db, mu: new(sync.RWMutex)}
			query.table = &table
			tableFlag = false
		}
		if databaseFlag {
			table := k3Table{name: "", database: part, mu: nil}
			query.table = &table
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
			return nil, errors.New(invalidSQLSyntax)
		}
		switch strings.ToUpper(fieldsParts[1]) {
		case "INT":
			fields[fieldsParts[0]] = k3INT
		case "FLOAT":
			fields[fieldsParts[0]] = k3FLOAT
		case "TEXT":
			fields[fieldsParts[0]] = k3TEXT
		default:
			return nil, errors.New(fmt.Sprintf("Invalid type: %s", fieldsParts[i+1]))
		}
		queryFields[i] = fieldsParts[0]
	}
	query.fields = fields
	query.table.fields = queryFields
	return query, nil
}

func parseInsertQuery(queryStr, db string) (*k3InsertQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(k3InsertQuery)
	intoFlag := false
	valuesFlag := false
	fieldsFlag := false
	valuesStr := ""
	fieldsStr := ""
	for _, part := range parts {
		if strings.EqualFold(part, "into") {
			intoFlag = true
			continue
		} else if strings.EqualFold(part, "values") {
			fieldsFlag = false
			valuesFlag = true
			continue
		}
		if intoFlag {
			table, ok := k3Tables[db+"."+part]
			if ok {
				query.table = table
			} else {
				return nil, errors.New(tableNotExists)
			}
			intoFlag = false
			fieldsFlag = true
			continue
		}
		if valuesFlag {
			valuesStr += part
		}
		if fieldsFlag {
			fieldsStr += part
		}
	}
	fieldsStr = strings.TrimSuffix(fieldsStr, ")")
	fieldsStr = strings.TrimPrefix(fieldsStr, "(")
	fieldsSlice := strings.Split(fieldsStr, ",")
	valuesSlice := make([]string, strings.Count(valuesStr, "("))
	str := ""
	cnt := 0
	flag := false
	for _, el := range valuesStr {
		if el == '(' {
			flag = true
			continue
		}
		if el == ')' {
			valuesSlice[cnt] = str
			str = ""
			cnt++
			flag = false
			continue
		}
		if flag {
			str += string(el)
		}
	}
	tmpMap := make([]map[string]string, len(valuesSlice))
	cnt = 0
	for _, value := range valuesSlice {
		tmpMap[cnt] = make(map[string]string, len(fieldsSlice))
		valueParts := strings.Split(value, ",")
		if len(fieldsSlice) != len(valueParts) {
			return nil, errors.New(invalidSQLSyntax)
		}
		for i := 0; i < len(fieldsSlice); i++ {
			tmpMap[cnt][fieldsSlice[i]] = valueParts[i]
		}
		cnt++
	}
	query.values = tmpMap
	return query, nil
}

func parseDropQuery(queryStr, db string) (*k3Table, error) {
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
				return nil, errors.New(invalidSQLSyntax)
			}
			continue
		} else if strings.EqualFold(part, "not") {
			if ifFlag {
				return nil, errors.New(invalidSQLLogic)
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
			table, ok := k3Tables[db+"."+part]
			if ok {
				return table, nil
			} else {
				return nil, errors.New(tableNotExists)
			}
		}
	}
	return nil, errors.New(invalidSQLSyntax)
}

func parseSelectQuery(queryStr, db string) (*k3SelectQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(k3SelectQuery)
	query.values = make([]string, 0)
	query.conditions = make([]Condition, 0)

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
				query.values = append(query.values, part)
			} else if fromCond {
				table, ok := k3Tables[db+"."+part]
				if !ok {
					return nil, errors.New(tableNotExists)
				}
				query.table = table
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
		query.conditions = conditions
	}

	return query, nil
}

func parseWhereClause(whereClause string) ([]Condition, error) {
	var conditions []Condition
	andParts := strings.Split(whereClause, "AND")
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

func parseSingleCondition(condStr string) (Condition, error) {
	if likeIdx := strings.Index(strings.ToUpper(condStr), "LIKE "); likeIdx >= 0 {
		column := strings.TrimSpace(condStr[:likeIdx])
		value := strings.TrimSpace(condStr[likeIdx+5:])

		if len(value) > 0 && (value[0] == '\'' || value[0] == '"') {
			value = strings.Trim(value, "'\"")
		}

		return Condition{
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

			return Condition{
				Column:   column,
				Operator: op,
				Value:    value,
			}, nil
		}
	}

	return Condition{}, errors.New(invalidSQLSyntax)
}

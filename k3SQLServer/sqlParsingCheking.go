package k3SQLServer

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const databaseDefaultName = "k3db"

type k3Table struct {
	database string
	name     string
	//fields map[string]int
	mu *sync.RWMutex
}

type k3join struct {
	src       string
	dst       string
	condition string
	typeJoin  int
}

type k3SelectQuery struct {
	table     *k3Table
	values    []string
	condition string
	//join      *k3join
}

type k3CreateQuery struct {
	table       *k3Table
	fields      map[string]int
	constraints map[string]string
}

type k3InsertQuery struct {
	table  *k3Table
	values []map[string]string
}

func checkQuery(queryStr string) bool {
	parts := strings.Fields(queryStr)
	part := parts[0]
	switch strings.ToLower(part) {
	case "select":
		return checkSelectQuery(parts)
	case "create":
		return checkCreateQuery(parts)
	case "drop":
		return checkDropQuery(parts)
	case "insert":
		return checkInsertQuery(parts)
	case "update":
		return checkUpdateQuery(parts)
	case "alter":
		return checkAlterQuery(parts)
	case "explain":
		return checkQuery(queryStr[len(part):])
	default:
		return false
	}
}

func checkSelectQuery(parts []string) bool {
	selectFlag := false
	fromFlag := false
	joinFlag := false
	orderFlag := false
	for _, part := range parts {
		if strings.EqualFold(part, "select") {
			selectFlag = true
		} else if strings.EqualFold(part, "from") {
			if !selectFlag {
				return false
			}
			fromFlag = true
		} else if strings.EqualFold(part, "join") {
			if !selectFlag || !fromFlag {
				return false
			}
			joinFlag = true
		} else if strings.EqualFold(part, "on") {
			if !selectFlag || !fromFlag || !joinFlag {
				return false
			}
		} else if strings.EqualFold(part, "where") {
			if !selectFlag || !fromFlag {
				return false
			}
		} else if strings.EqualFold(part, "order") {
			if !selectFlag || !fromFlag {
				return false
			}
			orderFlag = true
		} else if strings.EqualFold(part, "by") {
			if !selectFlag || !fromFlag || !orderFlag {
				return false
			}
		}
	}
	if selectFlag && fromFlag {
		return true
	}
	return false
}

func checkCreateQuery(parts []string) bool {
	return true
}

func checkDropQuery(parts []string) bool {
	return true
}

func checkInsertQuery(parts []string) bool {
	return true
}

func checkUpdateQuery(parts []string) bool {
	return true
}

func checkAlterQuery(parts []string) bool {
	return true
}

func parseSelectQuery(queryStr string) (*k3SelectQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(k3SelectQuery)
	join := new(k3join)
	query.values = make([]string, 0)
	selectCond := false
	fromCond := false
	whereCond := false
	joinCond := false
	joinFlag := false
	onCond := false
	onFlag := false
	for _, part := range parts {
		if strings.EqualFold(part, "select") {
			selectCond = true
			continue
		} else if strings.EqualFold(part, "from") {
			fromCond = true
			selectCond = false
			continue
		} else if strings.EqualFold(part, "join") {
			joinCond = true
			joinFlag = true
			continue
		} else if strings.EqualFold(part, "on") {
			joinCond = false
			onCond = true
			onFlag = true
			continue
		} else if strings.EqualFold(part, "where") {
			onCond = false
			whereCond = true
			continue
		}
		if selectCond {
			query.values = append(query.values, part)
		} else if fromCond {
			table, ok := k3Tables[part]
			if ok {
				query.table = table
			} else {
				return nil, errors.New(tableNotExists)
			}
			fromCond = false
		} else if whereCond {
			query.condition += part
		} else if joinCond {
			join.src = query.table.name
			join.dst = part
		} else if onCond {
			join.condition += part
		}
	}
	if (joinFlag && !onFlag) || (!joinFlag && onFlag) || len(query.values) == 0 {
		return nil, errors.New("SQL syntax error")
	}
	//if !joinFlag {
	//	query.join = nil
	//} else {
	//	query.join = join
	//}
	query.table.database = databaseDefaultName
	return query, nil
}

func parseCreateQuery(queryStr string) (*k3CreateQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(k3CreateQuery)
	tableFlag := false
	ifFlag := false
	notFlag := false
	for _, part := range parts {
		if strings.EqualFold(part, "table") {
			tableFlag = true
			continue
		} else if strings.EqualFold(part, "if") {
			ifFlag = true
			continue
		} else if strings.EqualFold(part, "not") {
			if !ifFlag {
				return nil, errors.New("SQL syntax error")
			}
			notFlag = true
			continue
		} else if strings.EqualFold(part, "exists") {
			if !ifFlag {
				return nil, errors.New("SQL syntax error")
			}
			if !notFlag {
				return nil, errors.New("SQL logic error")
			}
			continue
		}
		if tableFlag {
			table := k3Table{name: part, database: databaseDefaultName, mu: new(sync.RWMutex)}
			k3Tables[part] = &table
			query.table = &table
			tableFlag = false
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
	for i := 0; i < len(fieldsPartsTypes); i++ {
		fieldsParts := strings.Fields(fieldsPartsTypes[i])
		if len(fieldsParts) != 2 {
			return nil, errors.New("SQL syntax error")
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
	}
	query.fields = fields
	return query, nil
}

func parseInsertQuery(queryStr string) (*k3InsertQuery, error) {
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
			table, ok := k3Tables[part]
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
		for i := 0; i < len(fieldsSlice); i++ {
			tmpMap[cnt][fieldsSlice[i]] = valueParts[i]
		}
		cnt++
	}
	query.values = tmpMap
	return query, nil
}

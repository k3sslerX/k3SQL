package k3SQLServer

import (
	"errors"
	"fmt"
	"strings"
)

const DatabaseDefaultName = "k3db"

type K3join struct {
	Src       string
	Dst       string
	Condition string
	TypeJoin  int
}

type K3SelectQuery struct {
	Database  string
	Table     string
	Values    []string
	Condition string
	Join      *K3join
}

type K3CreateQuery struct {
	Database    string
	Table       string
	Fields      map[string]int
	Constraints map[string]string
}

type K3InsertQuery struct {
	Database string
	Table    string
	Values   []map[string]string
}

func CheckQuery(queryStr string) bool {
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
		return CheckQuery(queryStr[len(part):])
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

func ParseSelectQuery(queryStr string) (*K3SelectQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(K3SelectQuery)
	join := new(K3join)
	query.Values = make([]string, 0)
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
			query.Values = append(query.Values, part)
		} else if fromCond {
			query.Table = part
			fromCond = false
		} else if whereCond {
			query.Condition += part
		} else if joinCond {
			join.Src = query.Table
			join.Dst = part
		} else if onCond {
			join.Condition += part
		}
	}
	if (joinFlag && !onFlag) || (!joinFlag && onFlag) || len(query.Values) == 0 || len(query.Table) == 0 {
		return nil, errors.New("SQL syntax error")
	}
	if !joinFlag {
		query.Join = nil
	} else {
		query.Join = join
	}
	query.Database = DatabaseDefaultName
	return query, nil
}

func ParseCreateQuery(queryStr string) (*K3CreateQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(K3CreateQuery)
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
			query.Table = part
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
			fields[fieldsParts[0]] = K3INT
		case "FLOAT":
			fields[fieldsParts[0]] = K3FLOAT
		case "TEXT":
			fields[fieldsParts[0]] = K3TEXT
		default:
			return nil, errors.New(fmt.Sprintf("Invalid type: %s", fieldsParts[i+1]))
		}
	}
	query.Fields = fields
	query.Database = DatabaseDefaultName
	return query, nil
}

func ParseInsertQuery(queryStr string) (*K3InsertQuery, error) {
	parts := strings.Fields(queryStr)
	query := new(K3InsertQuery)
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
			query.Table = part
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
	query.Values = tmpMap
	query.Database = DatabaseDefaultName
	return query, nil
}

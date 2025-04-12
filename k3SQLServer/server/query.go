package server

import (
	"fmt"
	"k3SQLServer/core"
	"k3SQLServer/parser"
	"regexp"
	"strings"
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
		case "delete":
			return checkDeleteQuery(queryStr)
		case "alter":
			return checkAlterQuery(queryStr)
		case "explain":
			return checkQuery(queryStr[len(part):])
		case "user":
			return checkUserQuery(queryStr)
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
	updateRegex := regexp.MustCompile(`(?is)^\s*UPDATE\s+\w+\s+SET\s+[^;]+(?:\s+WHERE\s+[^;]+)?\s*;?\s*$`)
	return updateRegex.MatchString(query)
}

func checkDeleteQuery(query string) bool {
	deleteRegex := regexp.MustCompile(`(?is)^\s*DELETE\s+FROM\s+\w+(?:\s+WHERE\s+[^;]+)?\s*;?\s*$`)
	return deleteRegex.MatchString(query)
}

func checkAlterQuery(query string) bool {
	return true
}

func checkUserQuery(query string) bool {
	return true
}

func querySQL(queryString, user string, dbSlice ...string) *k3QueryResponse {
	db := core.DatabaseDefaultName
	if len(dbSlice) > 0 {
		db = dbSlice[0]
	}
	queryString = strings.ToLower(queryString)
	response := &k3QueryResponse{}
	response.RespType = "query"
	response.Status = false
	if !checkQuery(queryString) {
		response.Error = core.InvalidSQLSyntax
		return response
	}
	queryParts := strings.Fields(queryString)
	switch strings.ToLower(queryParts[0]) {
	case "select":
		query, err := parser.ParseSelectQuery(queryString, db)
		if err == nil {
			resp, rows, err := core.SelectTable(query, user)
			response.Fields = resp
			if err == nil {
				response.Status = true
				response.TableFields = query.Table.Fields
				response.Message = fmt.Sprintf("%d rows found", rows)
			} else {
				response.Error = err.Error()
			}
		} else {
			response.Error = err.Error()
		}
		return response
	case "create":
		query, err := parser.ParseCreateQuery(queryString, db)
		if err == nil {
			if len(query.Table.Name) > 0 {
				err = core.CreateTable(query)
				if err == nil {
					response.Status = true
					response.Message = "done"
				} else {
					response.Error = err.Error()
				}
			} else {
				err = core.CreateDatabase(query.Table.Database)
				if err == nil {
					response.Status = true
					response.Message = "done"
				} else {
					response.Error = err.Error()
				}
			}
		} else {
			response.Error = err.Error()
		}
		return response
	case "insert":
		query, err := parser.ParseInsertQuery(queryString, db)
		if err == nil {
			err = core.InsertTable(query, user)
			if err == nil {
				response.Status = true
				response.Message = "done"
			} else {
				response.Error = err.Error()
			}
		} else {
			response.Error = err.Error()
		}
		return response
	case "update":
		query, err := parser.ParseUpdateQuery(queryString, db)
		if err == nil {
			count, err := core.UpdateTable(query, user)
			if err == nil {
				response.Status = true
				response.Message = fmt.Sprintf("%d rows updated", count)
			} else {
				response.Error = err.Error()
			}
		} else {
			response.Error = err.Error()
		}
		return response
	case "drop":
		table, err := parser.ParseDropQuery(queryString, db)
		if err == nil {
			err = core.DropTable(table, user)
			if err == nil {
				response.Status = true
				response.Message = "done"
			} else {
				response.Error = err.Error()
			}
		} else {
			response.Error = err.Error()
		}
		return response
	case "delete":
		query, err := parser.ParseDeleteQuery(queryString, db)
		if err == nil {
			count, err := core.DeleteTable(query, user)
			if err == nil {
				response.Status = true
				response.Message = fmt.Sprintf("%d rows deleted", count)
			} else {
				response.Error = err.Error()
			}
		} else {
			response.Error = err.Error()
		}
		return response
	case "user":
		query, err := parser.ParseUserQuery(queryString, db)
		if err == nil {
			err = core.ProcessUser(query)
			if err == nil {
				response.Status = true
				response.Message = "done"
			} else {
				response.Error = err.Error()
			}
		} else {
			response.Error = err.Error()
		}
		return response
	default:
		response.Error = core.InvalidSQLSyntax
		return response
	}
}

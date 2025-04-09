package k3SQLServer

import (
	"fmt"
	"strings"
)

func querySQL(queryString string, dbSlice ...string) *k3QueryResponse {
	db := databaseDefaultName
	if len(dbSlice) > 0 {
		db = dbSlice[0]
	}
	queryString = strings.ToLower(queryString)
	response := &k3QueryResponse{}
	response.RespType = "query"
	response.Status = false
	if !checkQuery(queryString) {
		response.Error = invalidSQLSyntax
		return response
	}
	queryParts := strings.Fields(queryString)
	switch strings.ToLower(queryParts[0]) {
	case "select":
		query, err := parseSelectQuery(queryString, db)
		if err == nil {
			resp, rows, err := selectTable(query)
			response.Fields = resp
			if err == nil {
				response.Status = true
				response.Message = fmt.Sprintf("%d rows found", rows)
			} else {
				response.Error = err.Error()
			}
		}
		return response
	case "create":
		query, err := parseCreateQuery(queryString, db)
		if err == nil {
			if len(query.table.name) > 0 {
				err = createTable(query)
				if err == nil {
					response.Status = true
					response.Message = "done"
				} else {
					response.Error = err.Error()
				}
			} else {
				err = createDatabase(query.table.database)
				if err == nil {
					response.Status = true
					response.Message = "done"
				} else {
					response.Error = err.Error()
				}
			}
		}
		return response
	case "insert":
		query, err := parseInsertQuery(queryString, db)
		if err == nil {
			err = insertTable(query)
			if err == nil {
				response.Status = true
				response.Message = "done"
			} else {
				response.Error = err.Error()
			}
		}
		return response
	case "update":
		query, err := parseUpdateQuery(queryString, db)
		if err == nil {
			count, err := updateTable(query)
			if err == nil {
				response.Status = true
				response.Message = fmt.Sprintf("%d rows updated", count)
			} else {
				response.Error = err.Error()
			}
		}
		return response
	case "drop":
		table, err := parseDropQuery(queryString, db)
		if err == nil {
			err = dropTable(table)
			if err == nil {
				response.Status = true
				response.Message = "done"
			} else {
				response.Error = err.Error()
			}
		}
		return response
	case "delete":
		query, err := parseDeleteQuery(queryString, db)
		if err == nil {
			count, err := deleteTable(query)
			if err == nil {
				response.Status = true
				response.Message = fmt.Sprintf("%d rows deleted", count)
			} else {
				response.Error = err.Error()
			}
		}
		return response
	case "user":
		query, err := parseUserQuery(queryString, db)
		if err == nil {
			err = processUser(query)
			if err == nil {
				response.Status = true
				response.Message = "done"
			} else {
				response.Error = err.Error()
			}
		}
		return response
	default:
		response.Error = invalidSQLSyntax
		return response
	}
}

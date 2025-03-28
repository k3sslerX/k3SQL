package k3SQLServer

import (
	"errors"
	"fmt"
	"strings"
)

func querySQL(queryString string, dbSlice ...string) (string, error) {
	db := databaseDefaultName
	if len(dbSlice) > 0 {
		db = dbSlice[0]
	}
	queryString = strings.ToLower(queryString)
	if !checkQuery(queryString) {
		return "error", errors.New(invalidSQLSyntax)
	}
	queryParts := strings.Fields(queryString)
	switch strings.ToLower(queryParts[0]) {
	case "select":
		query, err := parseSelectQuery(queryString, db)
		if err == nil {
			resp, err := selectTable(query)
			if err == nil {
				out := parseOutput(resp, query.table)
				return out, err
			}
		}
		return "error", err
	case "create":
		query, err := parseCreateQuery(queryString, db)
		if err == nil {
			if len(query.table.name) > 0 {
				err = createTable(query)
				if err == nil {
					return "done", err
				}
			} else {
				err = createDatabase(query.table.database)
				if err == nil {
					return "done", err
				}
			}
		}
		return "error", err
	case "insert":
		query, err := parseInsertQuery(queryString, db)
		if err == nil {
			err = insertTable(query)
			if err == nil {
				return "done", err
			}
		}
		return "error", err
	case "drop":
		table, err := parseDropQuery(queryString, db)
		if err == nil {
			err = dropTable(table)
			if err == nil {
				return "done", err
			}
		}
		return "error", err
	case "delete":
		query, err := parseDeleteQuery(queryString, db)
		if err == nil {
			count, err := deleteTable(query)
			if err == nil {
				return fmt.Sprintf("%d rows deleted", count), nil
			}
		}
		return "error", err
	default:
		return "error", errors.New(invalidSQLSyntax)
	}
}

func parseOutput(resp []map[string]string, table *k3Table) string {
	if len(resp) > 0 {
		fields := make([]string, len(resp[0]))
		cnt := 0
		str := "|"
		for _, k := range table.fields {
			if _, ok := resp[0][k]; ok {
				fields[cnt] = k
				str += fmt.Sprintf(" %10s |", k)
				cnt++
			}
		}
		str += "\n|"
		str += strings.Repeat("-", 10*cnt+3*cnt-1)
		str += "|\n"
		for i := 0; i < len(resp); i++ {
			str += "|"
			for j := 0; j < len(fields); j++ {
				str += fmt.Sprintf(" %10s |", resp[i][fields[j]])
			}
			str += "\n"
		}
		return str
	}
	return "empty result"
}

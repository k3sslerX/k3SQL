package k3SQLServer

import (
	"errors"
	"strings"
)

func Query(queryString string) error {
	if !checkQuery(queryString) {
		return errors.New("SQL invalid syntax")
	}
	queryParts := strings.Fields(queryString)
	switch strings.ToLower(queryParts[0]) {
	case "select":
		query, err := parseSelectQuery(queryString)
		if err == nil {
			err = selectTable(query)
		}
		return err
	case "create":
		query, err := parseCreateQuery(queryString)
		if err == nil {
			err = createTable(query)
		}
		return err
	case "insert":
		query, err := parseInsertQuery(queryString)
		if err == nil {
			err = insertTable(query)
		}
		return err
	default:
		return errors.New("SQL invalid syntax")
	}
}

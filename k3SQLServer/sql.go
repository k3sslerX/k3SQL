package k3SQLServer

import (
	"errors"
	"fmt"
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
			resp, err := selectTable(query)
			if err == nil {
				fmt.Println(parseOutput(resp))
			}
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

// THIS FUNCTION WOULD BE IN CLIENT-SIDE; ONLY FOR TEST HERE
func parseOutput(resp []map[string]string) string {
	if len(resp) > 0 {
		fields := make([]string, len(resp[0]))
		cnt := 0
		str := "|"
		for k, _ := range resp[0] {
			fields[cnt] = k
			str += fmt.Sprintf(" %10s |", k)
			cnt++
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

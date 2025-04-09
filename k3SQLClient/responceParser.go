package k3SQLClient

import (
	"fmt"
	"strings"
)

func parseOutput(resp []map[string]string, msg string, tableFields []string) string {
	var str string
	if len(resp) > 0 {
		fields := make([]string, len(resp[0]))
		cnt := 0
		str += "|"
		for _, k := range tableFields {
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
	}
	if msg == "" {
		str += "empty result\n"
	}
	str += msg + "\n"
	return str
}

package k3SQLServer

func CreateTable(query *K3CreateQuery) error {
	return createTableFile(query.Table, query.Fields)
}

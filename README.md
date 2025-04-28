# k3SQL

k3SQL is a DBMS based on client-server architecture, using the method of concurency queries (as opposed to parallel ones, as in Postgres), which allows not to limit the number of simultaneous queries by the number of processor threads.

## k3SQLClient

⚠️ k3SQLClient is here only for tests. Soon, it will be converted to a driver that satisfies the driver interface of the sql package of the Golang standard library.

## TODO

| task                  | status |
|-----------------------|--------|
| create query          | ✅      |
| insert query          | ✅      |
| drop query            | ✅      |
| select query          | ✅      |
| conditional select    | ✅      |
| update query          | ✅      |
| conditional update    | ✅      |
| delete query          | ✅      |
| conditional delete    | ✅      |
| alter query           | ❌      |
| tables constraints    | ❌      |
| user table creating   | ✅      |
| mutex support         | ✅      |
| tables encrypting     | ❌      |
| indexing optimization | ❌      |
| parts optimization    | ❌      |
| meta data query       | ❌      |
| reliability           | ❌      |
| transactions          | ❌      |
| user authentication   | ✅      |

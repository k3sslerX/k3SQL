# k3SQL

k3SQL is a DBMS based on client-server architecture, using the method of concurency queries (as opposed to parallel ones, as in Postgres), which allows not to limit the number of simultaneous queries by the number of processor threads.

## Query-formats

Change the connection request & response form (remove strings, add json)


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

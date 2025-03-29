# k3SQL

**k3SQL** - СУБД, основанная на клиент-серверной архитектуре, использующая метод конкурентных запросов (в отличии от параллельных, как в Postgres), что позволяет не ограничивать количество одновременных запросов количеством потоков процессора.

## Server

TODO:

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

## Client

| task                      | status |
|---------------------------|--------|
| connect to server         | ✅      |
| manipulate db from client | ❌      |
| user support              | ❌      |
| user authentication       | ✅      |

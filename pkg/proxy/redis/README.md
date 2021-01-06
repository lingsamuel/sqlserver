# SQL Syntax to Redis

Assume redis key format: `PREFIX_KEYNAME`.

We treat `PREFIX` as table name in SQL.


```sql
use redis;
set source="tcp://172.26.0.1:6379";
CREATE TABLE prefix(K TEXT, V TEXT);

SELECT V FROM prefix WHERE K='key';
```

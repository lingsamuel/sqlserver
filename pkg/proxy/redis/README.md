# SQL Syntax to Redis

Assume redis key format: `PREFIX_KEYNAME`.

We treat `PREFIX` as table name in SQL.

Assume your docker0 ip is `172.26.0.1`.

```bash
# start redis server
redis-server --bind 0.0.0.0

# init test data
redis-cli set prefix_key value

# start mysql proxy
make run
```

```sql
use redis;
set source="tcp://172.26.0.1:63790";
CREATE TABLE prefix(K TEXT, V TEXT);

SELECT V FROM prefix WHERE K='key'; -- equals redis GET prefix_key
```

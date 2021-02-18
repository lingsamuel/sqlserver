# SQL Syntax to Redis

Assume redis key format: `PREFIX_KEYNAME`.

We treat `PREFIX` as table name in SQL.

Assume your docker0 ip is `172.26.0.1`.

```bash
# start redis server
redis-server --bind 0.0.0.0

# init test data
redis-cli set prefix_key value
redis-cli set prefix_key2 value2
redis-cli set prefix_int_key 1
redis-cli set prefix_int_key2 2

# start mysql proxy
make run
```

# Support Data Types

## Strings -> TEXT

```sql
use redis;
set source="tcp://172.26.0.1:6379";
CREATE TABLE prefix(K TEXT, V TEXT);

SELECT V FROM prefix WHERE K='key' or K='key2'; -- GET prefix_key
```

## Strings -> INTEGER

```sql
use redis;
set source="tcp://172.26.0.1:6379";
CREATE TABLE prefix_int(K TEXT, V INTEGER);

SELECT V FROM prefix_int WHERE K='key' or K='key2'; -- GET prefix_int_key
```

## Hashes -> TEXT

```sql
use redis;
set source="tcp://172.26.0.1:6379";
CREATE TABLE prefix_hash(K TEXT, FIELD TEXT, V TEXT);

SELECT V FROM prefix_int WHERE K='key'; -- HGETALL prefix_hash_key 
SELECT V FROM prefix_int WHERE K='key' AND FIELD='username'; -- HGET prefix_hash_key username 
SELECT V FROM prefix_int WHERE K='key' AND FIELD='username' OR FIELD='password'; -- HGET prefix_hash_key username; HGET prefix_hash_key password 
```

## Lists -> TEXT

```sql
use redis;
set source="tcp://172.26.0.1:6379";
CREATE TABLE prefix_list(K TEXT, V TEXT);

SELECT V FROM prefix_int WHERE K='key'; -- HGETALL prefix_list_key 
```

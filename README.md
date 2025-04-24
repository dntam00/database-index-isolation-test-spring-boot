"# database-index-isolation-test-spring-boot" 


## Lock query

```sql
SELECT index_name, lock_type, lock_mode, lock_data, thread_id
FROM performance_schema.data_locks
ORDER BY index_name, lock_data DESC;
```

![lock-img](./img/img.png)

To enable mysql command log:

```bash
SET GLOBAL general_log = 'ON';
SET GLOBAL general_log_file = '/var/lib/mysql/general.log';
```


To view mysql command log:

```bash
docker exec -it mysql-db-1 tail -f /var/lib/mysql/general.log
```

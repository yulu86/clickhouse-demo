# ClickHouse 去重 Demo

## 1. 启动ClickHouse
```
docker run -d --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

## 2. 打开客户端
```
docker exec -it some-clickhouse-server clickhouse-client
```

## 3. 尽情玩耍
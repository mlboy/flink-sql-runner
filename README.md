# Flink SQL Runner

轻量级 Flink 应用，用于在 FlinkSessionJob 中执行 SQL。平台通过 K8s CRD 传入 `--sql` 参数，本 JAR 解析并通过 Flink TableEnvironment 执行。

## 架构

```
平台后端 ──创建CRD──▶ FlinkSessionJob ──启动──▶ flink-sql-runner.jar
                      spec:                      ├─ 解析 --sql 参数
                        deploymentName: session   ├─ 分割多条 SQL
                        job:                      └─ 逐条 executeSql()
                          jarURI: .../flink-sql-runner.jar
                          args: ["--sql", "CREATE TABLE ..."]
```

## 构建

### 从 GitHub 下载（推荐）

- **每次 push/PR 构建**：打开仓库 **Actions** 页，进入最新一次 “Build and Publish JAR” 运行，在页面底部 **Artifacts** 区下载 `flink-sql-runner-jar`。
- **Release 版本**：在 **Releases** 页选择版本（如 `v1.0.0`），直接下载附件中的 `flink-sql-runner-*.jar`。

### 本地构建

```bash
# 需要 JDK 11+ 和 Maven 3.6+
mvn clean package -DskipTests

# 产物位于
ls target/flink-sql-runner-1.0.0.jar
```

## 使用方式

### 方式一：--sql（推荐，平台使用此方式）

```bash
# 直接传入 SQL 语句（多条用分号分隔）
flink run \
  --target kubernetes-session \
  -Dkubernetes.cluster-id=my-session \
  flink-sql-runner-1.0.0.jar \
  --sql "CREATE TABLE src (...) WITH (...); INSERT INTO sink SELECT * FROM src;"
```

### 方式二：--sql-file

```bash
# 从文件读取 SQL
flink run \
  --target kubernetes-session \
  -Dkubernetes.cluster-id=my-session \
  flink-sql-runner-1.0.0.jar \
  --sql-file /opt/flink/usrlib/sql/job.sql
```

## 部署到 Flink Session 集群

### 1. 打入镜像（推荐）

```dockerfile
FROM flink:1.18.1
COPY target/flink-sql-runner-1.0.0.jar /opt/flink/usrlib/flink-sql-runner.jar
```

### 2. 或通过 PVC/ConfigMap 挂载

将 JAR 放到共享存储，在 FlinkDeployment 的 podTemplate 中挂载。

## FlinkSessionJob CRD 示例

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkSessionJob
metadata:
  name: my-sql-job
spec:
  deploymentName: flink-session-cluster
  job:
    jarURI: local:///opt/flink/usrlib/flink-sql-runner.jar
    args:
      - "--sql"
      - |
        CREATE TABLE source_table (
          user_id BIGINT,
          behavior STRING,
          ts TIMESTAMP(3),
          WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'kafka',
          'topic' = 'user_behavior',
          'properties.bootstrap.servers' = 'kafka:9092',
          'format' = 'json'
        );

        CREATE TABLE sink_table (
          window_start TIMESTAMP(3),
          behavior STRING,
          cnt BIGINT
        ) WITH (
          'connector' = 'jdbc',
          'url' = 'jdbc:mysql://mysql:3306/analytics',
          'table-name' = 'behavior_stats'
        );

        INSERT INTO sink_table
        SELECT
          TUMBLE_START(ts, INTERVAL '1' MINUTE),
          behavior,
          COUNT(*)
        FROM source_table
        GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), behavior;
```

## SQL 支持

支持所有 Flink SQL 语法：
- DDL: `CREATE TABLE`, `CREATE VIEW`, `DROP TABLE`, etc.
- DML: `INSERT INTO ... SELECT`, `SELECT` (结果输出到日志)
- SET: `SET 'key' = 'value'`
- USE: `USE CATALOG`, `USE DATABASE`

多条语句用 `;` 分隔，按顺序执行。

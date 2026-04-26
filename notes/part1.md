# Part 1 — Environment Setup

## Q1 — Roles of each component

### Kafka
Distributed publish/subscribe **message broker**. It decouples the producers
(things that generate data — here, the financial API ingestor) from the
consumers (things that process data — here, the Spark streaming job).
Messages are durable, ordered within a partition, and replayable, so Kafka
also acts as a short-term buffer that absorbs producer bursts and allows
consumers to fall behind or replay from an earlier offset.

### Spark Streaming (Structured Streaming)
Distributed **stream processing engine**. It continuously reads from Kafka
in micro-batches (or continuous mode), applies DataFrame/SQL transformations
(filters, windowed aggregations, joins, moving averages) and writes the
results out to a sink (HDFS, another Kafka topic, a database, the console).
It's the "compute" layer of the pipeline.

### HDFS
Distributed **file system / data lake**. It stores large files reliably
across many machines by splitting them into blocks and replicating each
block. In this lab it is the long-term storage for the Medallion layers
(bronze / silver / gold Parquet files) — i.e. the system of record that
Superset eventually queries.

### Superset
**Business Intelligence / visualization** tool. It connects to a SQL engine
(Hive, Trino, Spark SQL, Postgres…), lets us register datasets, build charts
(time series, bar, moving averages) and compose them into dashboards. It is
the presentation layer — what a business user sees.

## Q2 — Installation

Everything runs as containers declared in `docker-compose.yml` at the root
of this lab. Single command to start the whole stack:

```bash
docker-compose up -d
```

Services started (10 containers, all on the `lab3` bridge network):

| Service        | Image                                            | Ports (host) |
|----------------|--------------------------------------------------|--------------|
| HDFS namenode  | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | 9870         |
| HDFS datanode1 | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | —            |
| HDFS datanode2 | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | —            |
| Zookeeper      | `confluentinc/cp-zookeeper:7.5.0`                | 2181         |
| Kafka broker   | `confluentinc/cp-kafka:7.5.0`                    | 9092, 29092  |
| Kafka UI       | `provectuslabs/kafka-ui:latest`                  | 8085         |
| Spark master   | `apache/spark:3.5.0`                             | 7077, 8080   |
| Spark worker 1 | `apache/spark:3.5.0`                             | —            |
| Spark worker 2 | `apache/spark:3.5.0`                             | —            |
| Spark client   | `apache/spark:3.5.0`                             | 4040         |
| Superset       | `apache/superset:3.1.1`                          | 8088         |

Two Kafka listeners are defined:
- `kafka:9092` — used by containers on the `lab3` network (Spark jobs, etc.)
- `localhost:29092` — used from the host machine (e.g. a Python producer
  running outside Docker)

## Q3 — Verification

### Kafka is running
```bash
docker exec lab3-kafka kafka-broker-api-versions --bootstrap-server kafka:9092
```
Expected: a list of API versions supported by the broker. A connection error
instead means Kafka or Zookeeper is not up.

Also useful:
- Kafka UI at http://localhost:8085 (shows cluster, topics, consumer groups)
- `docker exec lab3-kafka kafka-topics --bootstrap-server kafka:9092 --list`

### HDFS is accessible
```bash
docker exec lab3-namenode hdfs dfsadmin -report
```
Expected: a report listing 2 live datanodes and the configured capacity.

Also:
- Namenode Web UI at http://localhost:9870 → `Datanodes` tab should show
  `datanode1` and `datanode2` as `In service`
- `docker exec lab3-namenode hdfs dfs -ls /` should return the root listing
  (empty on first start)

### Spark is correctly configured
```bash
docker exec lab3-spark-client bash -c \
  "echo 'println(spark.version)' | /opt/spark/bin/spark-shell --master spark://spark-master:7077"
```
Expected output includes `version 3.5.0` and
`Spark context available as 'sc' (master = spark://spark-master:7077, app id = app-...)`.

Also:
- Spark Master UI at http://localhost:8080 — should list 2 registered Workers
  with ~1 core and a few GB of memory each
- Spark Application UI at http://localhost:4040 while a job is running

### Superset is correctly configured
- UI at http://localhost:8088 — login page returns HTTP 200
- Credentials: `admin` / `admin` (created automatically by the container
  entrypoint on first boot)
- Health endpoint: `curl http://localhost:8088/health` → `OK`

## Q4 — Kafka topic creation

```bash
docker exec lab3-kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic financial_data \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000
```

### How many partitions, and why?

We chose **3 partitions**. The rationale:

1. **Parallelism ceiling.** The number of partitions is the maximum number
   of consumers (inside one consumer group) that can read a topic in
   parallel. More partitions = more potential consumer parallelism.
2. **Match the compute side.** Our Spark cluster has 2 workers; 3 is a small
   multiple that gives Spark room to parallelize without over-shredding the
   data.
3. **Match the symbol cardinality.** We plan to stream a small basket of
   symbols (e.g. BTCUSDT, ETHUSDT, SOLUSDT). Using the symbol as the message
   key + 3 partitions means ticks for the same symbol always land on the
   same partition — which gives us per-symbol ordering for free.
4. **Don't over-provision.** On a single-broker demo, 1 partition would work
   but serializes all reads; many partitions (e.g. 30) just add metadata
   overhead with no benefit on a single broker. 3 is the sweet spot for a lab.

Replication factor is **1** because we only have one broker. In production
it would be 3.

Retention is **7 days** (`604800000 ms`), which is plenty for a lab session
and for replaying data into Spark if we need to rebuild HDFS layers.

### Verifying the topic
```bash
docker exec lab3-kafka kafka-topics \
  --bootstrap-server kafka:9092 --describe --topic financial_data
```
Result observed:
```
Topic: financial_data  PartitionCount: 3  ReplicationFactor: 1
  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### End-to-end smoke test
Produce one message then consume it from the beginning:
```bash
echo '{"test":"hello from lab3"}' | docker exec -i lab3-kafka \
  kafka-console-producer --bootstrap-server kafka:9092 --topic financial_data

docker exec lab3-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic financial_data \
  --from-beginning --max-messages 1
```
Result: the message came back through the consumer — Kafka plumbing works.

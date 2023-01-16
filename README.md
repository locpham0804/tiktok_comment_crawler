# tiktok_comment_crawler

## Tech

In this project, we use these technologies:
- Apache Kafka
- Postgres DB
- ClickHouse
- Debezium Connector
- Docker (for fast build and test)

Diagrams for pipeline:
![etl](/python/assets/etl.png)

## Features

- Connect directly to Tiktok channel, which is currently live. Then extract the comment and the information of the user commenting, and load the data into OLTP and OLAP for further uses.
- Debezium for capturing data change, and monitoring losses when running the pipeline.

## Installation

- Install environment: zookeeper, kafka, debezium, postgres, clickhouse

At the root folder

```
docker-compose up -d
```

Install dependencies

```
cd python
pip install -r requirements.txt
```

## Start crawling

Inside the python directory, open a terminal and run:
```
python3 kafka_producer.py "<unique_name_tiktok_live>"
```
You can get "unique_name_tiktok_live" from tiktok live like this picture:

The comment is produce and store in kafka topic "raw_comment_tiktok"
Next, open another terminal and run:
```
python3 kafka_consumer_group.py
```
This will consume the message in topic "raw_comment_tiktok" and push data into postgres database

For clickhouse, use sql developer tool to connect, then run these SQL statement to consume message from kafka.
```
CREATE TABLE tiktokcomment (
    user_Id UInt64,
    unique_Id String,
    nickname String,
    follow_Role Int8,
    comment String,
    live_User_unique_Id String,
    time_created DateTime64
) Engine = MergeTree
PARTITION BY toYYYYMM(time_created)
ORDER BY (time_created);
```
```
CREATE TABLE kafka_consume_tiktok_comment (
    user_Id UInt64,
    unique_Id String,
    nickname String,
    follow_Role Int8,
    comment String,
    live_User_unique_Id String,
    time_created DateTime64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
       kafka_topic_list = 'raw_comment_tiktok',
       kafka_group_name = 'tiktok_consumer_group_olap',
       kafka_format = 'JSONEachRow';
```
```
CREATE MATERIALIZED VIEW kafka_consume_tiktok_comment_mv TO tiktok AS
SELECT 
    user_Id,
    unique_Id,
    nickname,
    follow_Role,
    comment,
    live_User_unique_Id,
    time_created
FROM kafka_consume_tiktok_comment;
```

At this point, use select SQL to check for data has been pushed to clickhouse
```
SELECT count() FROM tiktok;
```

For Debezium connector, first we need to create debezium connector to capture data change for specific table in postgres. Open a terminal in root dicrectory and run:
```
curl -i -X POST -H "Accept:Application/json" -H "Content-Type:application/json" 127.0.0.1:8083/connectors/ --data "@debezium.json"
```
The connector has been created. Now everytime "tiktokcomment" table in postgres has an update, the update information will send to kafka under topic "postgres.public.tiktokcomment"

Continue to run a terminal with this command to monitor data to check synchronize between kafka and postgres.
```
python3 kafka_consumer.py
```


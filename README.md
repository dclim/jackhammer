# Jackhammer
Generates and pushes random event data into Kafka to be used for load testing the performance of Druid's [Kafka indexing service](http://druid.io/docs/0.9.2/development/extensions-core/kafka-ingestion.html).

## Build
```
mvn package
```

## Extract
```
tar xzf target/jackhammer-1.0-SNAPSHOT-bin.tar.gz
```

## Run
```
jackhammer-1.0-SNAPSHOT/jackhammer --help
```

## Notes
  * Make sure you have sufficient event generators / Kafka brokers / bandwidth so that your results actually reflect the performance of the Kafka indexing tasks.
  * It may be beneficial to run the event generators first to pre-load Kafka with data before starting up the Kafka supervisor and running the load test.
  * The supervisor status endpoint `GET /druid/indexer/v1/supervisor/<supervisorId>/status` is useful for determining ingestion rate by comparing the currentOffset values vs. time.

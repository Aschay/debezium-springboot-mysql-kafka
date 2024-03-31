# Change data capture with embedded debezium ,mysql and springboot

Implementing data replications with cdc design patter via an embedded debezium server that sits on top on kafka (event streaming platerfom) and create events to

listen to our app-db (mysql database) changes (delete,update and adding operations) at row-level using transactional logs and replicate it in our cdc-db (mysql database )

## 1.Configuring the embedded debezium 

### adding the embedded server to our app

```xml
<dependency>
	<groupId>io.debezium</groupId>
	<artifactId>debezium-api</artifactId>
	<version>${debezium.version}</version>
</dependency>
<dependency>
	<groupId>io.debezium</groupId>
	<artifactId>debezium-embedded</artifactId>
	<version>${debezium.version}</version>
</dependency>
```
### track the change to our mysql database 
```xml
<dependency>
	<groupId>io.debezium</groupId>
	<artifactId>debezium-connector-mysql</artifactId>
	<version>${debezium.version}</version>
</dependency>
```
### configuring debezium 
```java
@Bean
	public io.debezium.config.Configuration customerConnector() throws IOException {
		File offsetStorageTempFile = File.createTempFile("offsets_", ".dat");
		File dbHistoryTempFile = File.createTempFile("dbhistory_", ".dat");
		File schHistoryTempFile = File.createTempFile("schemahistory_", ".dat");
		return io.debezium.config.Configuration.create()
				.with("name", "customer-mysql-connector")
				.with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
				.with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
				.with("offset.storage.file.filename", offsetStorageTempFile.getAbsolutePath())
				.with("offset.flush.interval.ms", "60000").with("include.schema.changes", "false")
				.with("database.hostname", appDbHost)
				.with("database.port", appDbPort)
				.with("database.user", appDbUsername)
				.with("database.password", appDbPassword)
				.with("database.dbname", appDbName)
				.with("database.include.list", appDbName)
				.with("database.allowPublicKeyRetrieval", "true")
				.with("database.server.id", "20183")
				.with("database.server.name", "cdc-db-server")
				.with("topic.prefix", "database-db-changes")
				.with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
				.with("schema.history.internal.file.filename", schHistoryTempFile.getAbsolutePath())
				.with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
				.with("database.history.file.filename", dbHistoryTempFile.getAbsolutePath())
				.build();
	}
```

### setup debezium engine to listen to every insertion(update/add) or deletion changes with
 ```java
public DebeziumListener(Configuration appdbConnectorConfiguration, CDCservice service) {
	this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
				                        .using(appdbConnectorConfiguration.asProperties())
				                        .notifying(this::handleChangeEvent).build();

	this.service = service;
	}
```
and 
```java
private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent) {
	SourceRecord sourceRecord = sourceRecordRecordChangeEvent.record();
	Struct sourceRecordValue = (Struct) sourceRecord.value();
	if (sourceRecordValue != null) {
		Operation operation = Operation.forCode((String) sourceRecordValue.get(OPERATION));
		if (operation != Operation.READ) {
			Map<String, Object> message;
			String record = AFTER; // For Update & Add operations.
			if (operation == Operation.DELETE) {
				record = BEFORE; // For Delete operations.
			}
			Struct struct = (Struct) sourceRecordValue.get(record);
			message = struct.schema().fields()
						              .stream().map(Field::name)
						               .filter(fieldName -> struct.get(fieldName) != null)
						.map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
						.collect(toMap(Pair::getKey, Pair::getValue));
			this.service.replicateData(message, operation);
			log.info("Data Changed: {} with Operation: {}", message, operation.name());
			}
		}
	}
```

### debezium  with kafka connect 
Unlike debezium embedded ,debezium with Kafka (Connect) offer more gain in the level of fault-tolerance and scalability.
you can use bitmani(smaller container) confulentinc or redpanda images depending on your needs :

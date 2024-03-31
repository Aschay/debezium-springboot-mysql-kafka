package com.aschay.debeziumDemo.config;

import java.io.File;
import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DebeziumConfig {

	@Value("${app-db.datasource.host}")
	private String customerDbHost;

	@Value("${app-db.datasource.database}")
	private String customerDbName;

	@Value("${app-db.datasource.port}")
	private String customerDbPort;

	@Value("${app-db.datasource.username}")
	private String customerDbUsername;

	@Value("${app-db.datasource.password}")
	private String customerDbPassword;

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
				
				.with("database.hostname", customerDbHost)
				.with("database.port", customerDbPort)
				.with("database.user", customerDbUsername)
				.with("database.password", customerDbPassword)
				.with("database.dbname", customerDbName)
				.with("database.include.list", customerDbName)
				.with("database.allowPublicKeyRetrieval", "true")
				.with("database.server.id", "10181")
				.with("database.server.name", "customer-mysql-db-server")
				.with("topic.prefix", "database-customer-changes")
				.with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
				.with("schema.history.internal.file.filename", schHistoryTempFile.getAbsolutePath())
				.with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
				.with("database.history.file.filename", dbHistoryTempFile.getAbsolutePath())
				.build();
	}


}
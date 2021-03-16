/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium SQL Server connector.
 *
 * @author Jiri Pechanec
 */
public class SqlServerChangeTableSetIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createMultipleTestDatabases();
        connection = TestHelper.testConnection();
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute(
                    "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                    "CREATE TABLE tableb (id int primary key, colb varchar(30))",
                    "CREATE TABLE tablec (id int primary key, colc varchar(30))");
            TestHelper.enableTableCdc(connection, databaseName, "tablea");
            TestHelper.enableTableCdc(connection, databaseName, "tableb");
        });

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        // Testing.Debug.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void addTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b')");
            }

            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);

            // Enable CDC for already existing table
            TestHelper.enableTableCdc(connection, databaseName, "tablec");

            // CDC for newly added table
            connection.execute(
                    "CREATE TABLE tabled (id int primary key, cold varchar(30))");
            TestHelper.enableTableCdc(connection, databaseName, "tabled");

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START + i;
                connection.execute(
                        "INSERT INTO tablec VALUES(" + id + ", 'c')");
                connection.execute(
                        "INSERT INTO tabled VALUES(" + id + ", 'd')");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablec"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tabled"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tablec")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tablec", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colc", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });
            records.recordsForTopic(TestHelper.topicName(databaseName, "tabled")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tabled", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("cold", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });
        });
    }

    @Test
    public void removeTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_1 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b')");
            }

            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);

            // Disable CDC for a table
            TestHelper.disableTableCdc(connection, databaseName, "tableb");

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_2 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a2')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b2')");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).isNullOrEmpty();
        });
    }

    @Test
    public void addColumnToTableEndOfBatchWithoutLsnLimit() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();
        addColumnToTable(config, true);
    }

    @Test
    @FixFor("DBZ-3992")
    public void addColumnToTableEndOfBatchWithLsnLimit() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.MAX_TRANSACTIONS_PER_ITERATION, 1)
                .build();
        addColumnToTable(config, true);
    }

    @Test
    public void addColumnToTableMiddleOfBatchWithoutLsnLimit() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();
        addColumnToTable(config, false);
    }

    @Test
    @FixFor("DBZ-3992")
    public void addColumnToTableMiddleOfBatchWithLsnLimit() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.MAX_TRANSACTIONS_PER_ITERATION, 1)
                .build();
        addColumnToTable(config, true);
    }

    private void addColumnToTable(Configuration config, boolean pauseAfterCaptureChange) throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final int ID_START_4 = 10000;

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_1 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b')");
            }

            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });

            // Enable a second capture instance
            connection.execute("ALTER TABLE dbo.tableb ADD newcol INT NOT NULL DEFAULT 0");

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_2 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a2')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b2', 2)");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);

            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });

            TestHelper.enableTableCdc(connection, databaseName, "tableb", "after_change");
            if (pauseAfterCaptureChange) {
                Thread.sleep(5_000);
            }

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_3 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a3')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b3', 3)");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);

            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                                .field("newcol", SchemaBuilder.int32().defaultValue(0).build())
                                .build());
            });

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_4 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a4')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b4', 4)");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                                .field("newcol", SchemaBuilder.int32().defaultValue(0).build())
                                .build());
            });
        });
    }

    @Test
    public void removeColumnFromTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_1 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b')");
            }

            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });

            // Enable a second capture instance
            connection.execute("ALTER TABLE dbo.tableb DROP COLUMN colb");
            TestHelper.enableTableCdc(connection, databaseName, "tableb", "after_change");

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_2 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a2')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ")");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);

            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .build());
            });

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_3 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a3')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ")");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .build());
            });
        });
    }

    @Test
    @FixFor("DBZ-2716")
    public void removeColumnFromTableWithoutChangingCapture() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute("CREATE TABLE tableb2 (colb1 varchar(30), id int primary key, colb2 varchar(30))");
            TestHelper.enableTableCdc(connection, databaseName, "tableb2");
            connection.execute("ALTER TABLE dbo.tableb2 DROP COLUMN colb1");
        });

        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 1;
        final int ID_START_1 = 10;
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.tableb2")
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.COLUMN_INCLUDE_LIST, ".*id")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);
            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_1 + i;
                connection.execute(
                        "INSERT INTO tableb2 VALUES(" + id + ", 'b2')");
            }

            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb2"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb2")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb2", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .build());
            });
        });
    }

    @Test
    public void addColumnToTableWithParallelWrites() throws Exception {
        final int RECORDS_PER_TABLE = 20;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_1 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b')");
            }

            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });

            Executors.newSingleThreadExecutor().submit(() -> {
                try (JdbcConnection connection = TestHelper.testConnection()) {
                    connection.execute("USE " + databaseName);
                    for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                        final int id = ID_START_2 + i;
                        connection.execute(
                                "INSERT INTO tablea VALUES(" + id + ", 'a2')");
                        connection.execute(
                                "INSERT INTO tableb(id,colb) VALUES(" + id + ",'b')");
                        Thread.sleep(1000);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    throw new IllegalArgumentException(e);
                }
            });

            // Enable a second capture instance
            connection.execute("ALTER TABLE dbo.tableb ADD colb2 VARCHAR(32)");
            TestHelper.enableTableCdc(connection, databaseName, "tableb", "after_change");

            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);

            final AtomicInteger beforeChangeCount = new AtomicInteger();
            final AtomicInteger afterChangeCount = new AtomicInteger();
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                if (((Struct) record.value()).getStruct("after").schema().field("colb2") != null) {
                    afterChangeCount.incrementAndGet();
                }
                else {
                    beforeChangeCount.incrementAndGet();
                    Assertions.assertThat(afterChangeCount.intValue()).isZero();
                }
            });
            Assertions.assertThat(beforeChangeCount.intValue()).isPositive();
            Assertions.assertThat(afterChangeCount.intValue()).isPositive();

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_3 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a3')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b1', 'b2')");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                                .field("colb2", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });
        });
    }

    @Test
    public void readHistoryAfterRestart() throws Exception {
        final int RECORDS_PER_TABLE = 1;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_1 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b')");
            }

            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);

            // Enable a second capture instance
            connection.execute("ALTER TABLE dbo.tableb DROP COLUMN colb");
            TestHelper.enableTableCdc(connection, databaseName, "tableb", "after_change");

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_2 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a2')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ")");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
        });

        stopConnector();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_3 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a3')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ")");
            }
            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .build());
            });
        });

        // Validate history change types
        final DocumentReader reader = DocumentReader.defaultReader();
        final List<Document> changes = new ArrayList<>();
        IoUtil.readLines(TestHelper.DB_HISTORY_PATH, line -> {
            try {
                changes.add(reader.read(line));
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        // 3 tables from snapshot + 1 ALTER
        Assertions.assertThat(changes).hasSize((3 + 1) * TestHelper.TEST_DATABASES.size());

        Map<String, List<Document>> documentsByDatabase = new HashMap<>();
        changes.stream().forEach(document -> documentsByDatabase.computeIfAbsent(document.getString("databaseName"), key -> new ArrayList<>()).add(document));
        TestHelper.forEachDatabase(databaseName -> {
            final List<Document> databaseChanges = documentsByDatabase.get(databaseName);
            databaseChanges.subList(0, 3).forEach(change -> {
                final Array changeArray = change.getArray("tableChanges");
                Assertions.assertThat(changeArray.size()).isEqualTo(1);
                final String type = changeArray.get(0).asDocument().getString("type");
                Assertions.assertThat(type).isEqualTo("CREATE");
            });
            final Array changeArray = databaseChanges.get(3).getArray("tableChanges");
            Assertions.assertThat(changeArray.size()).isEqualTo(1);
            final String type = changeArray.get(0).asDocument().getString("type");
            final String tableIid = changeArray.get(0).asDocument().getString("id");
            Assertions.assertThat(type).isEqualTo("ALTER");
            Assertions.assertThat(tableIid).isEqualTo(String.format("\"%s\".\"dbo\".\"tableb\"", databaseName));
        });
    }

    @Test
    public void renameColumn() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_1 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b')");
            }

            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });

            // CDC must be disabled, otherwise rename fails
            TestHelper.disableTableCdc(connection, databaseName, "tableb");
            // Enable a second capture instance
            connection.execute("exec sp_rename 'tableb.colb', 'newcolb';");
            TestHelper.enableTableCdc(connection, databaseName, "tableb", "after_change");

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_2 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a2')");
                connection.execute(
                        "INSERT INTO tableb(id,newcolb) VALUES(" + id + ", 'b2')");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);

            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("newcolb", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_3 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a3')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", 'b3')");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("newcolb", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
            });
        });
    }

    @Test
    public void changeColumn() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final int ID_START_2 = 100;
        final int ID_START_3 = 1000;
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_1 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", '" + id + "')");
            }

            SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
                final Struct value = ((Struct) record.value()).getStruct("after");
                final int id = value.getInt32("id");
                final String colb = value.getString("colb");
                Assertions.assertThat(Integer.toString(id)).isEqualTo(colb);
            });

            // Enable a second capture instance
            connection.execute("ALTER TABLE dbo.tableb ALTER COLUMN colb INT");
            TestHelper.enableTableCdc(connection, databaseName, "tableb", "after_change");

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_2 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a2')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", '" + id + " ')");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);

            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_INT32_SCHEMA)
                                .build());
                final Struct value = ((Struct) record.value()).getStruct("after");
                final int id = value.getInt32("id");
                final int colb = value.getInt32("colb");
                Assertions.assertThat(id).isEqualTo(colb);
            });

            for (int i = 0; i < RECORDS_PER_TABLE; i++) {
                final int id = ID_START_3 + i;
                connection.execute(
                        "INSERT INTO tablea VALUES(" + id + ", 'a3')");
                connection.execute(
                        "INSERT INTO tableb VALUES(" + id + ", '" + id + " ')");
            }
            records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tablea"))).hasSize(RECORDS_PER_TABLE);
            Assertions.assertThat(records.recordsForTopic(TestHelper.topicName(databaseName, "tableb"))).hasSize(RECORDS_PER_TABLE);
            records.recordsForTopic(TestHelper.topicName(databaseName, "tableb")).forEach(record -> {
                assertSchemaMatchesStruct(
                        (Struct) ((Struct) record.value()).get("after"),
                        SchemaBuilder.struct()
                                .optional()
                                .name(TestHelper.schemaName(databaseName, "tableb", "Value"))
                                .field("id", Schema.INT32_SCHEMA)
                                .field("colb", Schema.OPTIONAL_INT32_SCHEMA)
                                .build());
                final Struct value = ((Struct) record.value()).getStruct("after");
                final int id = value.getInt32("id");
                final int colb = value.getInt32("colb");
                Assertions.assertThat(id).isEqualTo(colb);
            });
        });
    }

    @Test
    @FixFor("DBZ-1491")
    public void addDefaultValue() throws Exception {
        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);

            TestHelper.waitForStreamingStarted();
            TestHelper.waitForMaxLsnAvailable(connection, databaseName);

            connection.execute("ALTER TABLE dbo.tableb ADD DEFAULT ('default_value') FOR colb");
            TestHelper.enableTableCdc(connection, databaseName, "tableb", "after_change");

            connection.execute("INSERT INTO tableb VALUES('1', 'some_value')");
            TestHelper.waitForCdcRecord(connection, databaseName, "tableb", "after_change", rs -> rs.getInt("id") == 1);

            List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(TestHelper.topicName(databaseName, "tableb"));
            Assertions.assertThat(records).hasSize(1);
            Testing.debug("Records: " + records);
            Testing.debug("Value Schema: " + records.get(0).valueSchema());
            Testing.debug("Fields: " + records.get(0).valueSchema().fields());
            Testing.debug("After Schema: " + records.get(0).valueSchema().field("after").schema());
            Testing.debug("After Columns: " + records.get(0).valueSchema().field("after").schema().fields());

            Schema colbSchema = records.get(0).valueSchema().field("after").schema().field("colb").schema();
            Testing.debug("ColumnB Schema: " + colbSchema);
            Testing.debug("ColumnB Schema Default Value: " + colbSchema.defaultValue());
            Assertions.assertThat(colbSchema.defaultValue()).isNotNull();
            Assertions.assertThat(colbSchema.defaultValue()).isEqualTo("default_value");
        });
    }

    @Test
    @FixFor("DBZ-1491")
    public void alterDefaultValue() throws Exception {
        TestHelper.forEachDatabase(databaseName -> {
            connection.execute("USE " + databaseName);
            connection.execute("CREATE TABLE table_dv (id int primary key, colb varchar(30))");
            connection.execute("ALTER TABLE dbo.table_dv ADD CONSTRAINT DV_colb DEFAULT ('default_value') FOR colb");
            TestHelper.enableTableCdc(connection, databaseName, "table_dv");
        });

        final Configuration config = TestHelper.defaultMultiDatabaseConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        TestHelper.forEachDatabase(databaseName -> {
            TestHelper.waitForSnapshotToBeCompleted(databaseName);
            connection.execute("USE " + databaseName);
            connection.execute("INSERT INTO table_dv VALUES('1', 'some_value')");
            consumeRecordsByTopic(1);

            // Default value constraint cannot be modified. Drop existing and create a new one instead.
            connection.execute("ALTER TABLE dbo.table_dv DROP CONSTRAINT DV_colb");
            connection.execute("ALTER TABLE dbo.table_dv ADD DEFAULT ('new_default_value') FOR colb");
            TestHelper.enableTableCdc(connection, databaseName, "table_dv", "after_change");

            connection.execute("INSERT INTO table_dv VALUES('2', 'some_value2')");
            List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(TestHelper.topicName(databaseName, "table_dv"));
            Assertions.assertThat(records).hasSize(1);

            Schema colbSchema = records.get(0).valueSchema().field("after").schema().field("colb").schema();
            Assertions.assertThat(colbSchema.defaultValue()).isNotNull();
            Assertions.assertThat(colbSchema.defaultValue()).isEqualTo("new_default_value");
        });
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.ChangeTable;

/**
 * A class invoked by {@link StreamingChangeEventSource} to notify the database of events via stored
 * procedures
 *
 * @author Jacob Gminder
 */
public class SqlServerStreamingProgressDatabaseCallbacks implements
        SqlServerStreamingProgressListener {

    private final SqlServerConnection dataConnection;

    SqlServerStreamingProgressDatabaseCallbacks(final SqlServerConnection dataConnection) {
        this.dataConnection = dataConnection;
    }

    public void onReadingNewChangeTable(Partition partition, ChangeTable table) throws SQLException {
        SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
        dataConnection.callbackOnReadingNewChangeTable(sqlServerPartition, table);
    }

    @Override
    public void connected(boolean connected) {
        // Do nothing.
    }
}

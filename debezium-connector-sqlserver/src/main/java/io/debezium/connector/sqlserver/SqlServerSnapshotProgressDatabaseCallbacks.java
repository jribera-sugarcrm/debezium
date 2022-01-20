/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import io.debezium.DebeziumException;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * A class invoked by {@link SnapshotChangeEventSource} to notify the database of events via stored procedures
 *
 * @author Jacob Gminder
 */
public class SqlServerSnapshotProgressDatabaseCallbacks implements SnapshotProgressListener {
    private final SqlServerConnection dataConnection;

    public SqlServerSnapshotProgressDatabaseCallbacks(SqlServerConnection dataConnection) {
        this.dataConnection = dataConnection;
    }

    @Override
    public void snapshotStarted(Partition partition) {
        try {
            SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
            dataConnection.callbackOnSnapshotStarted(sqlServerPartition);
        }
        catch (SQLException exception) {
            throw new DebeziumException(exception);
        }
    }

    @Override
    public void snapshotCompleted(Partition partition) {
        try {
            SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
            dataConnection.callbackOnSnapshotCompleted(sqlServerPartition);
        }
        catch (SQLException exception) {
            throw new DebeziumException(exception);
        }
    }

    @Override
    public void snapshotAborted(Partition partition) {
        try {
            SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
            dataConnection.callbackOnSnapshotAborted(sqlServerPartition);
        }
        catch (SQLException exception) {
            throw new DebeziumException(exception);
        }
    }

    @Override
    public void dataCollectionSnapshotCompleted(Partition partition,
                                                DataCollectionId dataCollectionId, long numRows) {
        try {
            SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
            dataConnection.callbackOnDataCollectionSnapshotCompleted(sqlServerPartition, dataCollectionId);
        }
        catch (SQLException exception) {
            throw new DebeziumException(exception);
        }
    }

    @Override
    public void rowsScanned(Partition partition, TableId tableId, long numRows) {
        // Do nothing
    }

    @Override
    public void currentChunk(Partition partition, String chunkId, Object[] chunkFrom,
                             Object[] chunkTo) {
        // Do nothing
    }

    @Override
    public void currentChunk(Partition partition, String chunkId, Object[] chunkFrom,
                             Object[] chunkTo, Object[] tableTo) {
        // Do nothing
    }

    @Override
    public void monitoredDataCollectionsDetermined(Partition partition, Iterable iterable) {
        // Do nothing
    }
}

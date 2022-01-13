/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * A list of {@link SnapshotProgressListener}s specific to one database.
 *
 * @author Jacob Gminder
 */
public class SnapshotProgressListenerList implements SnapshotProgressListener {

    private final Iterable<SnapshotProgressListener> listeners;

    /**
     * Create a SqlServerSnapshotProgressListenerList to allow iterating over multiple snapshot listeners
     *
     * @param listeners
     *            A list of {@link SnapshotProgressListener} called for changes in the state of snapshot.
     *
     */
    public SnapshotProgressListenerList(Iterable<SnapshotProgressListener> listeners) {
        if (listeners == null) {
            throw new IllegalArgumentException("The list of snapshot progress listeners cannot be null.");
        }
        this.listeners = listeners;
    }

    @Override
    public void snapshotStarted(Partition partition) {
        listeners.forEach(listener -> listener.snapshotStarted(partition));
    }

    @Override
    public void monitoredDataCollectionsDetermined(Partition partition,
                                                   Iterable<? extends DataCollectionId> dataCollectionIds) {
        listeners.forEach(listener -> listener.monitoredDataCollectionsDetermined(partition, dataCollectionIds));
    }

    @Override
    public void snapshotCompleted(Partition partition) {
        listeners.forEach(listener -> listener.snapshotCompleted(partition));
    }

    @Override
    public void snapshotAborted(Partition partition) {
        listeners.forEach(listener -> listener.snapshotAborted(partition));
    }

    @Override
    public void dataCollectionSnapshotCompleted(Partition partition,
                                                DataCollectionId dataCollectionId, long numRows) {
        listeners.forEach(listener -> listener.dataCollectionSnapshotCompleted(partition, dataCollectionId, numRows));
    }

    @Override
    public void rowsScanned(Partition partition, TableId tableId, long numRows) {
        listeners.forEach(listener -> listener.rowsScanned(partition, tableId, numRows));
    }

    @Override
    public void currentChunk(Partition partition, String chunkId, Object[] chunkFrom,
                             Object[] chunkTo) {
        listeners.forEach(listener -> listener.currentChunk(partition, chunkId, chunkFrom, chunkTo));
    }

    @Override
    public void currentChunk(Partition partition, String chunkId, Object[] chunkFrom,
                             Object[] chunkTo, Object[] tableTo) {
        listeners.forEach(listener -> listener.currentChunk(partition, chunkId, chunkFrom, chunkTo, tableTo));
    }
}

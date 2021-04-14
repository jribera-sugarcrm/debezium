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
 * A class invoked by {@link SnapshotChangeEventSource} whenever an important event or change of state happens.
 *
 * @author Jiri Pechanec
 */
public interface SnapshotProgressListener {

    void snapshotStarted(Partition partition);

    void monitoredDataCollectionsDetermined(Partition partition, Iterable<? extends DataCollectionId> dataCollectionIds);

    void snapshotCompleted(Partition partition);

    void snapshotAborted(Partition partition);

    void dataCollectionSnapshotCompleted(Partition partition, DataCollectionId dataCollectionId, long numRows);

    void rowsScanned(Partition partition, TableId tableId, long numRows);

    void currentChunk(Partition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo);

    void currentChunk(Partition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo);

    static SnapshotProgressListener NO_OP = new SnapshotProgressListener() {
        @Override
        public void snapshotStarted(Partition partition) {
        }

        @Override
        public void rowsScanned(Partition partition, TableId tableId, long numRows) {
        }

        @Override
        public void monitoredDataCollectionsDetermined(Partition partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        }

        @Override
        public void dataCollectionSnapshotCompleted(Partition partition, DataCollectionId dataCollectionId, long numRows) {
        }

        @Override
        public void snapshotCompleted(Partition partition) {
        }

        @Override
        public void snapshotAborted(Partition partition) {
        }

        @Override
        public void currentChunk(Partition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        }

        @Override
        public void currentChunk(Partition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        }
    };
}

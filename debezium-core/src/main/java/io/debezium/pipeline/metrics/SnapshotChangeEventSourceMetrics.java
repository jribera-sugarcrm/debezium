/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Collection;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * Metrics related to the initial snapshot of a connector.
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class SnapshotChangeEventSourceMetrics
        extends PipelineMetrics<SnapshotChangeEventSourcePartitionMetrics>
        implements ChangeEventSourceTaskMetricsMXBean, SnapshotProgressListener {

    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                             EventMetadataProvider metadataProvider,
                                                                             Collection<? extends Partition> partitions) {
        super(taskContext, "snapshot", changeEventQueueMetrics, partitions,
                (partition) -> new SnapshotChangeEventSourcePartitionMetrics(taskContext, "snapshot",
                        partition, metadataProvider));
    }

    @Override
    public void monitoredDataCollectionsDetermined(Partition partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        onPartitionEvent(partition, bean -> bean.monitoredDataCollectionsDetermined(dataCollectionIds));
    }

    @Override
    public void dataCollectionSnapshotCompleted(Partition partition, DataCollectionId dataCollectionId, long numRows) {
        onPartitionEvent(partition, bean -> bean.dataCollectionSnapshotCompleted(dataCollectionId, numRows));
    }

    @Override
    public void snapshotStarted(Partition partition) {
        onPartitionEvent(partition, SnapshotChangeEventSourcePartitionMetrics::snapshotStarted);
    }

    @Override
    public void snapshotCompleted(Partition partition) {
        onPartitionEvent(partition, SnapshotChangeEventSourcePartitionMetrics::snapshotCompleted);
    }

    @Override
    public void snapshotAborted(Partition partition) {
        onPartitionEvent(partition, SnapshotChangeEventSourcePartitionMetrics::snapshotAborted);
    }

    @Override
    public void rowsScanned(Partition partition, TableId tableId, long numRows) {
        onPartitionEvent(partition, bean -> bean.rowsScanned(tableId, numRows));
    }

    public void currentChunk(Partition partition, String chunkId, Object[] from, Object[] to) {
        onPartitionEvent(partition, bean -> bean.currentChunk(chunkId, from, to));
    }

    public void currentChunk(Partition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        onPartitionEvent(partition, bean -> bean.currentChunk(chunkId, chunkFrom, chunkTo, tableTo));
    }
}

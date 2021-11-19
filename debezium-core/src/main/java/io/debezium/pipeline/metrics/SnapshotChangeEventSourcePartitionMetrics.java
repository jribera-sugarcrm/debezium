/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * Metrics related to the initial snapshot of a connector.
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class SnapshotChangeEventSourcePartitionMetrics extends ChangeEventSourcePartitionMetrics
        implements SnapshotChangeEventSourcePartitionMetricsMXBean {

    private final AtomicBoolean snapshotRunning = new AtomicBoolean();
    private final AtomicBoolean snapshotCompleted = new AtomicBoolean();
    private final AtomicBoolean snapshotAborted = new AtomicBoolean();
    private final AtomicLong startTime = new AtomicLong();
    private final AtomicLong stopTime = new AtomicLong();
    private final ConcurrentMap<String, Long> rowsScanned = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, String> remainingTables = new ConcurrentHashMap<>();

    private final Set<String> capturedTables = Collections.synchronizedSet(new HashSet<>());

    private final AtomicReference<String> chunkId = new AtomicReference<>();
    private final AtomicReference<Object[]> chunkFrom = new AtomicReference<>();
    private final AtomicReference<Object[]> chunkTo = new AtomicReference<>();
    private final AtomicReference<Object[]> tableFrom = new AtomicReference<>();
    private final AtomicReference<Object[]> tableTo = new AtomicReference<>();

    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourcePartitionMetrics(T taskContext, String contextName,
                                                                                      Partition partition,
                                                                                      EventMetadataProvider metadataProvider) {
        super(taskContext, contextName, partition, metadataProvider);
    }

    @Override
    public int getTotalTableCount() {
        return this.capturedTables.size();
    }

    @Override
    public int getRemainingTableCount() {
        return this.remainingTables.size();
    }

    @Override
    public boolean getSnapshotRunning() {
        return this.snapshotRunning.get();
    }

    @Override
    public boolean getSnapshotCompleted() {
        return this.snapshotCompleted.get();
    }

    @Override
    public boolean getSnapshotAborted() {
        return this.snapshotAborted.get();
    }

    @Override
    public long getSnapshotDurationInSeconds() {
        final long startMillis = startTime.get();
        if (startMillis <= 0L) {
            return 0;
        }
        long stopMillis = stopTime.get();
        if (stopMillis == 0L) {
            stopMillis = clock.currentTimeInMillis();
        }
        return (stopMillis - startMillis) / 1000L;
    }

    /**
     * @deprecated Superseded by the 'Captured Tables' metric. Use {@link #getCapturedTables()}.
     * Scheduled for removal in a future release.
     */
    @Override
    @Deprecated
    public String[] getMonitoredTables() {
        return capturedTables.toArray(new String[capturedTables.size()]);
    }

    @Override
    public String[] getCapturedTables() {
        return capturedTables.toArray(new String[capturedTables.size()]);
    }

    public void monitoredDataCollectionsDetermined(Iterable<? extends DataCollectionId> dataCollectionIds) {
        Iterator<? extends DataCollectionId> it = dataCollectionIds.iterator();
        while (it.hasNext()) {
            DataCollectionId dataCollectionId = it.next();

            this.remainingTables.put(dataCollectionId.identifier(), "");
            capturedTables.add(dataCollectionId.identifier());
        }
    }

    public void dataCollectionSnapshotCompleted(DataCollectionId dataCollectionId, long numRows) {
        rowsScanned.put(dataCollectionId.identifier(), numRows);
        remainingTables.remove(dataCollectionId.identifier());
    }

    public void snapshotStarted() {
        this.snapshotRunning.set(true);
        this.snapshotCompleted.set(false);
        this.snapshotAborted.set(false);
        this.startTime.set(clock.currentTimeInMillis());
        this.stopTime.set(0L);
    }

    public void snapshotCompleted() {
        this.snapshotCompleted.set(true);
        this.snapshotAborted.set(false);
        this.snapshotRunning.set(false);
        this.stopTime.set(clock.currentTimeInMillis());
    }

    public void snapshotAborted() {
        this.snapshotCompleted.set(false);
        this.snapshotAborted.set(true);
        this.snapshotRunning.set(false);
        this.stopTime.set(clock.currentTimeInMillis());
    }

    public void rowsScanned(TableId tableId, long numRows) {
        rowsScanned.put(tableId.toString(), numRows);
    }

    public ConcurrentMap<String, Long> getRowsScanned() {
        return rowsScanned;
    }

    public void currentChunk(String chunkId, Object[] from, Object[] to) {
        this.chunkId.set(chunkId);
        this.chunkFrom.set(from);
        this.chunkTo.set(to);
    }

    public void currentChunk(String chunkId, Object[] chunkFrom, Object[] chunkTo, Object tableTo[]) {
        currentChunk(chunkId, chunkFrom, chunkTo);
        this.tableFrom.set(chunkFrom);
        this.tableTo.set(tableTo);
    }

    @Override
    public String getChunkId() {
        return chunkId.get();
    }

    @Override
    public String getChunkFrom() {
        return Arrays.toString(chunkFrom.get());
    }

    @Override
    public String getChunkTo() {
        return Arrays.toString(chunkTo.get());
    }

    @Override
    public String getTableFrom() {
        return arrayToString(tableFrom.get());
    }

    @Override
    public String getTableTo() {
        return arrayToString(tableTo.get());
    }

    private String arrayToString(Object[] array) {
        return (array == null) ? null : Arrays.toString(array);
    }

    @Override
    public void reset() {
        snapshotRunning.set(false);
        snapshotCompleted.set(false);
        snapshotAborted.set(false);
        startTime.set(0);
        stopTime.set(0);
        rowsScanned.clear();
        remainingTables.clear();
        capturedTables.clear();
        chunkId.set(null);
        chunkFrom.set(null);
        chunkTo.set(null);
        tableFrom.set(null);
        tableTo.set(null);
    }
}
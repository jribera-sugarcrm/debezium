/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SqlServerStreamingExecutionContext {
    private final Queue<SqlServerChangeTable> schemaChangeCheckpoints;
    private final AtomicReference<SqlServerChangeTable[]> tablesSlot;
    private TxLogPosition lastProcessedPosition;
    private final AtomicBoolean changesStoppedBeingMonotonic;
    private boolean shouldIncreaseFromLsn;
    private boolean streamedEvents;

    public SqlServerStreamingExecutionContext(PriorityQueue<SqlServerChangeTable> schemaChangeCheckpoints, AtomicReference<SqlServerChangeTable[]> tablesSlot,
                                              TxLogPosition changePosition, AtomicBoolean changesStoppedBeingMonotonic, boolean snapshotCompleted) {
        this.schemaChangeCheckpoints = schemaChangeCheckpoints;
        this.tablesSlot = tablesSlot;
        this.changesStoppedBeingMonotonic = changesStoppedBeingMonotonic;
        this.shouldIncreaseFromLsn = snapshotCompleted;
        this.lastProcessedPosition = changePosition;
        this.streamedEvents = false;
    }

    public void setShouldIncreaseFromLsn(boolean shouldIncreaseFromLsn) {
        this.shouldIncreaseFromLsn = shouldIncreaseFromLsn;
    }

    public Queue<SqlServerChangeTable> getSchemaChangeCheckpoints() {
        return schemaChangeCheckpoints;
    }

    public AtomicReference<SqlServerChangeTable[]> getTablesSlot() {
        return tablesSlot;
    }

    public TxLogPosition getLastProcessedPosition() {
        return lastProcessedPosition;
    }

    public void setLastProcessedPosition(TxLogPosition lastProcessedPosition) {
        this.lastProcessedPosition = lastProcessedPosition;
    }

    public AtomicBoolean getChangesStoppedBeingMonotonic() {
        return changesStoppedBeingMonotonic;
    }

    public boolean getStreamedEvents() {
        return streamedEvents;
    }

    public void setStreamedEvents(boolean streamedEvents) {
        this.streamedEvents = streamedEvents;
    }

    public boolean getShouldIncreaseFromLsn() {
        return shouldIncreaseFromLsn;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import io.debezium.annotation.Immutable;
import io.debezium.relational.ChangeTable;
import io.debezium.relational.TableId;

/**
 * A logical representation of change table containing changes for a given source table.
 * There is usually one change table for each source table. When the schema of the source table
 * is changed then two change tables could be present.
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerChangeTable extends ChangeTable implements Comparable<SqlServerChangeTable> {

    private static final String CDC_SCHEMA = "cdc";

    /**
     * A LSN from which the data in the change table are relevant
     */
    private final Lsn startLsn;

    /**
     * A LSN to which the data in the change table are relevant
     */
    private Lsn stopLsn;

    /**
     * The date of the change table creation
     */
    private final Instant createDate;

    /**
     * List of columns captured by the CDC table.
     */
    @Immutable
    private final List<String> capturedColumns;

    public SqlServerChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId,
                                Lsn startLsn, Lsn stopLsn, Instant createDate,
                                List<String> capturedColumns) {
        super(captureInstance, sourceTableId, resolveChangeTableId(sourceTableId, captureInstance), changeTableObjectId);
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.createDate = createDate;
        this.capturedColumns = capturedColumns != null ? Collections.unmodifiableList(capturedColumns) : Collections.emptyList();
    }

    public SqlServerChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn,
                                Instant createDate) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn, createDate, null);
    }

    public Lsn getStartLsn() {
        return startLsn;
    }

    public Lsn getStopLsn() {
        return stopLsn;
    }

    public Instant getCreateDate() {
        return createDate;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.stopLsn = stopLsn;
    }

    public List<String> getCapturedColumns() {
        return capturedColumns;
    }

    @Override
    public String toString() {
        return "Capture instance \"" + getCaptureInstance() + "\" [sourceTableId=" + getSourceTableId()
                + ", changeTableId=" + getChangeTableId() + ", startLsn=" + startLsn + ", changeTableObjectId="
                + getChangeTableObjectId() + ", stopLsn=" + stopLsn + ", createDate=" + createDate + "]";
    }

    private static TableId resolveChangeTableId(TableId sourceTableId, String captureInstance) {
        return sourceTableId != null ? new TableId(sourceTableId.catalog(), CDC_SCHEMA, captureInstance + "_CT") : null;
    }

    @Override
    public int compareTo(SqlServerChangeTable o) {
        if (this == o) {
            return 0;
        }

        int comparison = getStartLsn().compareTo(o.getStartLsn());
        if (comparison != 0) {
            return comparison;
        }

        return getCreateDate().compareTo(o.getCreateDate());
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import io.debezium.pipeline.source.spi.StreamingProgressListener;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.ChangeTable;

public interface SqlServerStreamingProgressListener extends StreamingProgressListener {
    void onReadingNewChangeTable(Partition partition, ChangeTable table) throws SQLException;

    static SqlServerStreamingProgressListener NO_OP = new SqlServerStreamingProgressListener() {
        @Override
        public void onReadingNewChangeTable(Partition partition, ChangeTable table) {
        }

        @Override
        public void connected(boolean connected) {
        }
    };
}

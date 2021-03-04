/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.connector.common.Partition;
import io.debezium.util.Collect;

public class SqlServerPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";
    private static final String DATABASE_PARTITION_KEY = "database";

    private final String serverName;
    private final String databaseName;

    public SqlServerPartition(String serverName, String databaseName) {
        this.serverName = serverName;
        this.databaseName = databaseName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName, DATABASE_PARTITION_KEY, databaseName);
    }

    /**
     * Returns the SQL Server database name corresponding to the partition.
     */
    String getDatabaseName() {
        return databaseName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SqlServerPartition other = (SqlServerPartition) obj;
        return Objects.equals(serverName, other.serverName) && Objects.equals(databaseName, other.databaseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName, databaseName);
    }

    static class Provider implements Partition.Provider<SqlServerPartition> {
        private final SqlServerConnectorConfig connectorConfig;
        private final SqlServerConnection connection;

        Provider(SqlServerConnectorConfig connectorConfig, SqlServerConnection connection) {
            this.connectorConfig = connectorConfig;
            this.connection = connection;
        }

        @Override
        public Set<SqlServerPartition> getPartitions() {
            String serverName = connectorConfig.getLogicalName();

            String[] databaseNames = { connectorConfig.getDatabaseName() };

            return Arrays.stream(databaseNames)
                    .map(databaseName -> connection.retrieveRealDatabaseName())
                    .map(databaseName -> new SqlServerPartition(serverName, databaseName))
                    .collect(Collectors.toSet());
        }
    }
}

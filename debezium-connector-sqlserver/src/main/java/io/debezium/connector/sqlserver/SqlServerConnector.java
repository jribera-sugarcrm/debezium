/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.config.CommonConnectorConfig.TASK_ID;
import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.DATABASE_NAME;
import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.DATABASE_NAMES;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Strings;

/**
 * The main connector class used to instantiate configuration and execution classes
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnector.class);

    private Map<String, String> properties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SqlServerConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (!properties.containsKey(DATABASE_NAMES.name())) {
            if (maxTasks > 1) {
                throw new IllegalArgumentException("Only a single connector task may be started");
            }

            return Collections.singletonList(properties);
        }

        final Configuration config = Configuration.from(properties);
        final SqlServerConnectorConfig sqlServerConfig = new SqlServerConnectorConfig(config);
        try (SqlServerConnection connection = connect(sqlServerConfig)) {
            return buildTaskConfigs(connection, maxTasks);
        }
        catch (SQLException e) {
            throw new IllegalArgumentException("Could not build task configs", e);
        }
    }

    private List<Map<String, String>> buildTaskConfigs(SqlServerConnection connection, int maxTasks) {

        // Parse the database names property
        final List<String> databaseNames = Arrays.asList(properties.get(DATABASE_NAMES.name()).split(","));
        final List<String> realDatabaseNames = connection.retrieveRealOnlineDatabaseNames(databaseNames);
        if (realDatabaseNames.isEmpty()) {
            throw new IllegalArgumentException();
        }

        // Initialize the database list for each task
        List<List<String>> taskDatabases = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskDatabases.add(new ArrayList<>());
        }

        // Add each database to a task list via round-robin.
        for (int dbNamesIndex = 0; dbNamesIndex < realDatabaseNames.size(); dbNamesIndex++) {
            int taskIndex = dbNamesIndex % maxTasks;
            taskDatabases.get(taskIndex).add(realDatabaseNames.get(dbNamesIndex));
        }

        // Create a task config for each task, assigning each a list of database names.
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int taskIndex = 0; taskIndex < maxTasks; taskIndex++) {
            String databases = String.join(",", taskDatabases.get(taskIndex));
            Map<String, String> taskProperties = new HashMap<>(properties);
            taskProperties.put(DATABASE_NAMES.name(), databases);
            taskProperties.put(TASK_ID, String.valueOf(taskIndex));
            taskConfigs.add(Collections.unmodifiableMap(taskProperties));
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return SqlServerConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final SqlServerConnectorConfig sqlServerConfig = new SqlServerConnectorConfig(config);

        if (Strings.isNullOrEmpty(sqlServerConfig.getDatabaseName())) {
            throw new IllegalArgumentException("Either '" + DATABASE_NAME + "' or '" + DATABASE_NAMES
                    + "' option must be specified");
        }

        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final ConfigValue userValue = configValues.get(RelationalDatabaseConnectorConfig.USER.name());
        // Try to connect to the database ...
        try (SqlServerConnection connection = connect(sqlServerConfig)) {
            connection.execute("SELECT @@VERSION");
            LOGGER.debug("Successfully tested connection for {} with user '{}'", connection.connectionString(),
                    connection.username());
        }
        catch (Exception e) {
            LOGGER.error("Failed testing connection for {} with user '{}'", config.withMaskedPasswords(),
                    userValue, e);
            hostnameValue.addErrorMessage("Unable to connect. Check this and other connection properties. Error: "
                    + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(SqlServerConnectorConfig.ALL_FIELDS);
    }

    private SqlServerConnection connect(SqlServerConnectorConfig sqlServerConfig) {
        return new SqlServerConnection(sqlServerConfig.jdbcConfig(),
                sqlServerConfig.getSourceTimestampMode(), null,
                () -> getClass().getClassLoader(),
                Collections.emptySet(),
                sqlServerConfig.isMultiPartitionModeEnabled());
    }
}

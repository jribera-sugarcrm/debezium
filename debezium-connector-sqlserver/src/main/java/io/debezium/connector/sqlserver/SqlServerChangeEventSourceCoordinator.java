/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;

public class SqlServerChangeEventSourceCoordinator extends ChangeEventSourceCoordinator<SqlServerPartition, SqlServerOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerChangeEventSourceCoordinator.class);

    private final Clock clock;
    private final Duration pollInterval;
    private Optional<IncrementalSnapshotChangeEventSource<? extends DataCollectionId>> incrementalSnapshotChangeEventSource;

    public SqlServerChangeEventSourceCoordinator(Offsets<SqlServerPartition, SqlServerOffsetContext> previousOffsets, ErrorHandler errorHandler,
                                                 Class<? extends SourceConnector> connectorType,
                                                 CommonConnectorConfig connectorConfig,
                                                 ChangeEventSourceFactory<SqlServerPartition, SqlServerOffsetContext> changeEventSourceFactory,
                                                 ChangeEventSourceMetricsFactory changeEventSourceMetricsFactory, EventDispatcher<?> eventDispatcher,
                                                 DatabaseSchema<?> schema,
                                                 Clock clock) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema);
        this.clock = clock;
        this.pollInterval = connectorConfig.getPollInterval();
        this.incrementalSnapshotChangeEventSource = Optional.empty();
    }

    @Override
    protected void streamEvents(AtomicReference<LoggingContext.PreviousContext> previousLogContext, CdcSourceTaskContext taskContext, ChangeEventSourceContext context,
                                Offsets<SqlServerPartition, SqlServerOffsetContext> streamingOffsets)
            throws InterruptedException {
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);

        LOGGER.info("Starting streaming");

        streamingSource = changeEventSourceFactory.getStreamingChangeEventSource();
        eventDispatcher.setEventListener(streamingMetrics);
        streamingConnected(true);
        streamingSource.init();

        while (running) {
            boolean streamedEvents = false;
            for (Map.Entry<SqlServerPartition, SqlServerOffsetContext> entry : streamingOffsets.getOffsets().entrySet()) {
                SqlServerPartition partition = entry.getKey();
                SqlServerOffsetContext previousOffset = entry.getValue();

                if (running) {
                    previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));
                    streamEvents(context, partition, previousOffset);
                    if (((SqlServerStreamingChangeEventSource) streamingSource).eventsStreamed(partition)) {
                        streamedEvents = true;
                    }
                }
            }

            if (!streamedEvents) {
                metronome.pause();
            }
        }

        LOGGER.info("Finished streaming");
    }

    @Override
    protected void streamEvents(ChangeEventSource.ChangeEventSourceContext context, SqlServerPartition partition,
                                SqlServerOffsetContext offsetContext)
            throws InterruptedException {
        if (!incrementalSnapshotChangeEventSource.isPresent()) {
            incrementalSnapshotChangeEventSource = changeEventSourceFactory
                    .getIncrementalSnapshotChangeEventSource(offsetContext, snapshotMetrics, snapshotMetrics);
            eventDispatcher.setIncrementalSnapshotChangeEventSource(incrementalSnapshotChangeEventSource);
            incrementalSnapshotChangeEventSource.ifPresent(x -> x.init(partition, offsetContext));
        }

        streamingSource.execute(context, partition, offsetContext);
    }
}

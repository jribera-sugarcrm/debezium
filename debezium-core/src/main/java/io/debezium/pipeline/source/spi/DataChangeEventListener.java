/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.DataCollectionId;

/**
 * A class invoked by {@link EventDispatcher} whenever an event is available for processing.
 *
 * @author Jiri Pechanec
 *
 */
public interface DataChangeEventListener {

    /**
     * Invoked if an event is processed for a captured table.
     */
    void onEvent(Partition partition, DataCollectionId source, OffsetContext offset, Object key, Struct value) throws InterruptedException;

    /**
     * Invoked for events pertaining to non-captured tables.
     */
    void onFilteredEvent(Partition partition, String event);

    /**
     * Invoked for events that cannot be processed.
     */
    void onErroneousEvent(Partition partition, String event);

    /**
     * Invoked for events that represent a connector event.
     */
    void onConnectorEvent(Partition partition, ConnectorEvent event);

    static DataChangeEventListener NO_OP = new DataChangeEventListener() {
        @Override
        public void onFilteredEvent(Partition partition, String event) {
        }

        @Override
        public void onErroneousEvent(Partition partition, String event) {
        }

        @Override
        public void onConnectorEvent(Partition partition, ConnectorEvent event) {
        }

        @Override
        public void onEvent(Partition partition, DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        }
    };
}

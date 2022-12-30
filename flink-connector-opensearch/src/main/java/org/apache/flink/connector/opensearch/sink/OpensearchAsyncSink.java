/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.opensearch.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.http.HttpHost;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Apache Flink's Async Sink to insert or update data in an Opensearch index (see please {@link
 * OpensearchAsyncWriter}).
 *
 * @param <InputT> type of the records converted to Opensearch actions (instances of {@link
 *     DocSerdeRequest})
 * @see OpensearchAsyncSinkBuilder on how to construct a OpensearchAsyncSink
 */
@PublicEvolving
public class OpensearchAsyncSink<InputT> extends AsyncSinkBase<InputT, DocSerdeRequest<?>> {
    private static final long serialVersionUID = 1L;

    private final List<HttpHost> hosts;
    private final NetworkClientConfig networkClientConfig;

    /**
     * Constructor creating an Opensearch async sink.
     *
     * @param maxBatchSize the maximum size of a batch of entries that may be sent
     * @param maxInFlightRequests he maximum number of in flight requests that may exist, if any
     *     more in flight requests need to be initiated once the maximum has been reached, then it
     *     will be blocked until some have completed
     * @param maxBufferedRequests the maximum number of elements held in the buffer, requests to add
     *     elements will be blocked while the number of elements in the buffer is at the maximum
     * @param maxBatchSizeInBytes the maximum size of a batch of entries that may be sent to KDS
     *     measured in bytes
     * @param maxTimeInBufferMS the maximum amount of time an entry is allowed to live in the
     *     buffer, if any element reaches this age, the entire buffer will be flushed immediately
     * @param maxRecordSizeInBytes the maximum size of a record the sink will accept into the
     *     buffer, a record of size larger than this will be rejected when passed to the sink
     * @param elementConverter converting incoming records to Opensearch write document requests
     * @param hosts the reachable Opensearch cluster nodes
     * @param networkClientConfig describing properties of the network connection used to connect to
     *     the Opensearch cluster
     */
    OpensearchAsyncSink(
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            ElementConverter<InputT, DocSerdeRequest<?>> elementConverter,
            List<HttpHost> hosts,
            NetworkClientConfig networkClientConfig) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.hosts = checkNotNull(hosts);
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");
        this.networkClientConfig = checkNotNull(networkClientConfig);
    }

    /**
     * Create a {@link OpensearchAsyncSinkBuilder} to construct a new {@link OpensearchAsyncSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link OpensearchAsyncSinkBuilder}
     */
    public static <InputT> OpensearchAsyncSinkBuilder<InputT> builder() {
        return new OpensearchAsyncSinkBuilder<>();
    }

    @Internal
    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<DocSerdeRequest<?>>> createWriter(
            InitContext context) throws IOException {
        return new OpensearchAsyncWriter<>(
                context,
                getElementConverter(),
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                hosts,
                networkClientConfig,
                Collections.emptyList());
    }

    @Internal
    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<DocSerdeRequest<?>>> restoreWriter(
            InitContext context,
            Collection<BufferedRequestState<DocSerdeRequest<?>>> recoveredState)
            throws IOException {
        return new OpensearchAsyncWriter<>(
                context,
                getElementConverter(),
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                hosts,
                networkClientConfig,
                recoveredState);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<BufferedRequestState<DocSerdeRequest<?>>>
            getWriterStateSerializer() {
        return new OpensearchWriterStateSerializer();
    }
}

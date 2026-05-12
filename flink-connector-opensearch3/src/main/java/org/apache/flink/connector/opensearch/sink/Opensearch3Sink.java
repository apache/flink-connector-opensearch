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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.opensearch.sink.Opensearch3Writer.Opensearch3FailureHandler;

import org.apache.hc.core5.http.HttpHost;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink Sink to insert or update data in an OpenSearch 3.x index. The sink supports the following
 * delivery guarantees.
 *
 * <ul>
 *   <li>{@link DeliveryGuarantee#NONE} does not provide any guarantees: actions are flushed to
 *       OpenSearch only depending on the configurations of the bulk processor. In case of a
 *       failure, it might happen that actions are lost if the bulk processor still has buffered
 *       actions.
 *   <li>{@link DeliveryGuarantee#AT_LEAST_ONCE} on a checkpoint the sink will wait until all
 *       buffered actions are flushed to and acknowledged by OpenSearch. No actions will be lost but
 *       actions might be sent to OpenSearch multiple times when Flink restarts. These additional
 *       requests may cause inconsistent data in OpenSearch right after the restart, but eventually
 *       everything will be consistent again.
 * </ul>
 *
 * @param <IN> type of the records converted to OpenSearch actions
 * @see Opensearch3SinkBuilder on how to construct an Opensearch3Sink
 */
@PublicEvolving
public class Opensearch3Sink<IN> implements Sink<IN> {

    private final List<HttpHost> hosts;
    private final Opensearch3Emitter<? super IN> emitter;
    private final BulkProcessorConfig buildBulkProcessorConfig;
    private final NetworkClientConfig networkClientConfig;
    private final DeliveryGuarantee deliveryGuarantee;
    private final Opensearch3FailureHandler failureHandler;
    @Nullable private final Opensearch3HttpClientConfigCallback httpClientConfigCallback;

    Opensearch3Sink(
            List<HttpHost> hosts,
            Opensearch3Emitter<? super IN> emitter,
            DeliveryGuarantee deliveryGuarantee,
            BulkProcessorConfig buildBulkProcessorConfig,
            NetworkClientConfig networkClientConfig,
            Opensearch3FailureHandler failureHandler,
            @Nullable Opensearch3HttpClientConfigCallback httpClientConfigCallback) {
        this.hosts = checkNotNull(hosts);
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");
        this.emitter = checkNotNull(emitter);
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
        this.buildBulkProcessorConfig = checkNotNull(buildBulkProcessorConfig);
        this.networkClientConfig = checkNotNull(networkClientConfig);
        this.failureHandler = checkNotNull(failureHandler);
        this.httpClientConfigCallback = httpClientConfigCallback;
    }

    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        return new Opensearch3Writer<>(
                hosts,
                emitter,
                deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE,
                buildBulkProcessorConfig,
                networkClientConfig,
                context.metricGroup(),
                context.getMailboxExecutor(),
                failureHandler,
                httpClientConfigCallback);
    }
}

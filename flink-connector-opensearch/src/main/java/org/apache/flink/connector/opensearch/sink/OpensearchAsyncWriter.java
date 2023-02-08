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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Apache Flink's Async Sink Writer to insert or update data in an Opensearch index (see please
 * {@link OpensearchAsyncSink}).
 *
 * @param <InputT> type of the records converted to Opensearch actions (instances of {@link
 *     DocSerdeRequest})
 */
@Internal
class OpensearchAsyncWriter<InputT> extends AsyncSinkWriter<InputT, DocSerdeRequest<?>> {
    private static final Logger LOG = LoggerFactory.getLogger(OpensearchAsyncWriter.class);

    private final RestHighLevelClient client;
    private final Counter numRecordsOutErrorsCounter;
    private volatile boolean closed = false;

    private static final FatalExceptionClassifier OPENSEARCH_FATAL_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.createChain(
                    new FatalExceptionClassifier(
                            err ->
                                    err instanceof NoRouteToHostException
                                            || err instanceof ConnectException,
                            err ->
                                    new OpenSearchException(
                                            "Could not connect to Opensearch cluster using provided hosts",
                                            err)));

    /**
     * Constructor creating an Opensearch async writer.
     *
     * @param context the initialization context
     * @param elementConverter converting incoming records to Opensearch write document requests
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
     * @param hosts the reachable Opensearch cluster nodes
     * @param networkClientConfig describing properties of the network connection used to connect to
     *     the Opensearch cluster
     * @param initialStates the initial state of the sink
     */
    OpensearchAsyncWriter(
            Sink.InitContext context,
            ElementConverter<InputT, DocSerdeRequest<?>> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            List<HttpHost> hosts,
            NetworkClientConfig networkClientConfig,
            Collection<BufferedRequestState<DocSerdeRequest<?>>> initialStates) {
        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .build(),
                initialStates);

        this.client =
                new RestHighLevelClient(
                        configureRestClientBuilder(
                                RestClient.builder(hosts.toArray(new HttpHost[0])),
                                networkClientConfig));

        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        checkNotNull(metricGroup);

        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
    }

    @Override
    protected void submitRequestEntries(
            List<DocSerdeRequest<?>> requestEntries,
            Consumer<List<DocSerdeRequest<?>>> requestResult) {

        BulkRequest bulkRequest = new BulkRequest();
        requestEntries.forEach(r -> bulkRequest.add(r.getRequest()));

        final CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        client.bulkAsync(
                bulkRequest,
                RequestOptions.DEFAULT,
                new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse response) {
                        future.complete(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        future.completeExceptionally(e);
                    }
                });

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedBulkRequest(err, requestEntries, requestResult);
                    } else if (response.hasFailures()) {
                        handlePartiallyFailedBulkRequests(response, requestEntries, requestResult);
                    } else {
                        requestResult.accept(Collections.emptyList());
                    }
                });
    }

    @Override
    protected long getSizeInBytes(DocSerdeRequest<?> requestEntry) {
        return requestEntry.getRequest().ramBytesUsed();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;

            try {
                client.close();
            } catch (final IOException ex) {
                LOG.warn("Error while closing RestHighLevelClient instance", ex);
            }
        }
    }

    private boolean isRetryable(Throwable err) {
        // isFatal() is really isNotFatal()
        if (!OPENSEARCH_FATAL_EXCEPTION_CLASSIFIER.isFatal(err, getFatalExceptionCons())) {
            return false;
        }
        return true;
    }

    private void handleFullyFailedBulkRequest(
            Throwable err,
            List<DocSerdeRequest<?>> requestEntries,
            Consumer<List<DocSerdeRequest<?>>> requestResult) {
        final boolean retryable = isRetryable(err.getCause());

        LOG.warn(
                "Opensearch AsyncWwriter failed to persist {} entries (retryable = {})",
                requestEntries.size(),
                retryable,
                err);

        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (retryable) {
            requestResult.accept(requestEntries);
        }
    }

    private void handlePartiallyFailedBulkRequests(
            BulkResponse response,
            List<DocSerdeRequest<?>> requestEntries,
            Consumer<List<DocSerdeRequest<?>>> requestResult) {

        final List<DocSerdeRequest<?>> failedRequestEntries = new ArrayList<>();
        final BulkItemResponse[] items = response.getItems();

        for (int i = 0; i < items.length; i++) {
            if (items[i].getFailure() != null) {
                failedRequestEntries.add(DocSerdeRequest.from(requestEntries.get(i).getRequest()));
            }
        }

        numRecordsOutErrorsCounter.inc(failedRequestEntries.size());
        requestResult.accept(failedRequestEntries);
    }

    private static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, NetworkClientConfig networkClientConfig) {
        if (networkClientConfig.getConnectionPathPrefix() != null) {
            builder.setPathPrefix(networkClientConfig.getConnectionPathPrefix());
        }

        builder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    if (networkClientConfig.getPassword() != null
                            && networkClientConfig.getUsername() != null) {
                        final CredentialsProvider credentialsProvider =
                                new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(
                                AuthScope.ANY,
                                new UsernamePasswordCredentials(
                                        networkClientConfig.getUsername(),
                                        networkClientConfig.getPassword()));

                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }

                    if (networkClientConfig.isAllowInsecure().orElse(false)) {
                        try {
                            httpClientBuilder.setSSLContext(
                                    SSLContexts.custom()
                                            .loadTrustMaterial(new TrustAllStrategy())
                                            .build());
                        } catch (final NoSuchAlgorithmException
                                | KeyStoreException
                                | KeyManagementException ex) {
                            throw new IllegalStateException(
                                    "Unable to create custom SSL context", ex);
                        }
                    }

                    return httpClientBuilder;
                });
        if (networkClientConfig.getConnectionRequestTimeout() != null
                || networkClientConfig.getConnectionTimeout() != null
                || networkClientConfig.getSocketTimeout() != null) {
            builder.setRequestConfigCallback(
                    requestConfigBuilder -> {
                        if (networkClientConfig.getConnectionRequestTimeout() != null) {
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    networkClientConfig.getConnectionRequestTimeout());
                        }
                        if (networkClientConfig.getConnectionTimeout() != null) {
                            requestConfigBuilder.setConnectTimeout(
                                    networkClientConfig.getConnectionTimeout());
                        }
                        if (networkClientConfig.getSocketTimeout() != null) {
                            requestConfigBuilder.setSocketTimeout(
                                    networkClientConfig.getSocketTimeout());
                        }
                        return requestConfigBuilder;
                    });
        }
        return builder;
    }
}

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
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.http.HttpHost;
import org.opensearch.action.DocWriteRequest;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder to construct an Opensearch compatible {@link OpensearchAsyncSink}.
 *
 * <p>The following example shows the minimal setup to create a OpensearchAsyncSink that submits
 * actions with the default number of actions to buffer (1000).
 *
 * <pre>{@code
 * OpensearchAsyncSink<Tuple2<String, String>> sink = OpensearchAsyncSink
 *     .<Tuple2<String, String>>builder()
 *     .setHosts(new HttpHost("localhost:9200")
 *     .setElementConverter((element, context) ->
 *         new IndexRequest("my-index").id(element.f0.toString()).source(element.f1));
 *     .build();
 * }</pre>
 *
 * @param <InputT> type of the records converted to Opensearch actions
 */
@PublicEvolving
public class OpensearchAsyncSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<
                InputT, DocSerdeRequest<?>, OpensearchAsyncSinkBuilder<InputT>> {
    private List<HttpHost> hosts;
    private String username;
    private String password;
    private String connectionPathPrefix;
    private Integer connectionTimeout;
    private Integer connectionRequestTimeout;
    private Integer socketTimeout;
    private Boolean allowInsecure;
    private ElementConverter<InputT, DocSerdeRequest<?>> elementConverter;

    /**
     * Sets the element converter.
     *
     * @param elementConverter element converter
     */
    public OpensearchAsyncSinkBuilder<InputT> setElementConverter(
            ElementConverter<InputT, DocWriteRequest<?>> elementConverter) {
        this.elementConverter =
                (element, context) ->
                        DocSerdeRequest.from(elementConverter.apply(element, context));
        return this;
    }

    /**
     * Sets the hosts where the Opensearch cluster nodes are reachable.
     *
     * @param hosts http addresses describing the node locations
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkArgument(hosts.length > 0, "Hosts cannot be empty.");
        this.hosts = Arrays.asList(hosts);
        return this;
    }

    /**
     * Sets the username used to authenticate the connection with the Opensearch cluster.
     *
     * @param username of the Opensearch cluster user
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setConnectionUsername(String username) {
        checkNotNull(username);
        checkArgument(!username.trim().isEmpty(), "Username cannot be empty");
        this.username = username;
        return this;
    }

    /**
     * Sets the password used to authenticate the conection with the Opensearch cluster.
     *
     * @param password of the Opensearch cluster user
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setConnectionPassword(String password) {
        checkNotNull(password);
        checkArgument(!password.trim().isEmpty(), "Password cannot be empty");
        this.password = password;
        return this;
    }

    /**
     * Sets a prefix which used for every REST communication to the Opensearch cluster.
     *
     * @param prefix for the communication
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setConnectionPathPrefix(String prefix) {
        checkNotNull(prefix);
        this.connectionPathPrefix = prefix;
        return this;
    }

    /**
     * Sets the timeout for requesting the connection of the Opensearch cluster from the connection
     * manager.
     *
     * @param timeout timeout for the connection request (in milliseconds)
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setConnectionRequestTimeout(int timeout) {
        checkArgument(
                timeout >= 0, "Connection request timeout must be larger than or equal to 0.");
        this.connectionRequestTimeout = timeout;
        return this;
    }

    /**
     * Sets the timeout for establishing a connection of the Opensearch cluster.
     *
     * @param timeout timeout for the connection (in milliseconds)
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setConnectionTimeout(int timeout) {
        checkArgument(timeout >= 0, "Connection timeout must be larger than or equal to 0.");
        this.connectionTimeout = timeout;
        return this;
    }

    /**
     * Sets the timeout for establishing a connection of the Opensearch cluster.
     *
     * @param timeout timeout for the connection (in milliseconds)
     * @param timeUnit timeout time unit
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setConnectionTimeout(int timeout, TimeUnit timeUnit) {
        checkNotNull(timeUnit, "TimeUnit cannot be null.");
        return setConnectionTimeout((int) timeUnit.toMillis(timeout));
    }

    /**
     * Sets the timeout for waiting for data or, put differently, a maximum period inactivity
     * between two consecutive data packets.
     *
     * @param timeout timeout for the socket (in milliseconds)
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setSocketTimeout(int timeout) {
        checkArgument(timeout >= 0, "Socket timeout must be larger than or equal to 0.");
        this.socketTimeout = timeout;
        return this;
    }

    /**
     * Sets the timeout for waiting for data or, put differently, a maximum period inactivity
     * between two consecutive data packets.
     *
     * @param timeout timeout for the socket
     * @param timeUnit timeout time unit
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setSocketTimeout(int timeout, TimeUnit timeUnit) {
        checkNotNull(timeUnit, "TimeUnit cannot be null.");
        return setSocketTimeout((int) timeUnit.toMillis(timeout));
    }

    /**
     * Allows to bypass the certificates chain validation and connect to insecure network endpoints
     * (for example, servers which use self-signed certificates).
     *
     * @param allowInsecure allow or not to insecure network endpoints
     * @return this builder
     */
    public OpensearchAsyncSinkBuilder<InputT> setAllowInsecure(boolean allowInsecure) {
        this.allowInsecure = allowInsecure;
        return this;
    }

    @Override
    public OpensearchAsyncSink<InputT> build() {
        return new OpensearchAsyncSink<InputT>(
                nonNullOrDefault(
                        getMaxBatchSize(),
                        1000), /* OpensearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION */
                nonNullOrDefault(
                        getMaxInFlightRequests(), 1), /* BulkProcessor::concurrentRequests */
                nonNullOrDefault(getMaxBufferedRequests(), 10000),
                nonNullOrDefault(
                        getMaxBatchSizeInBytes(),
                        2 * 1024
                                * 1024), /* OpensearchConnectorOptions.BULK_FLUSH_MAX_SIZE_OPTION */
                nonNullOrDefault(
                        getMaxTimeInBufferMS(),
                        1000), /* OpensearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION */
                nonNullOrDefault(getMaxRecordSizeInBytes(), 1 * 1024 * 1024), /* 1Mb */
                elementConverter,
                hosts,
                buildNetworkClientConfig());
    }

    private static int nonNullOrDefault(Integer value, int defaultValue) {
        return (value != null) ? value : defaultValue;
    }

    private static long nonNullOrDefault(Long value, long defaultValue) {
        return (value != null) ? value : defaultValue;
    }

    private NetworkClientConfig buildNetworkClientConfig() {
        return new NetworkClientConfig(
                username,
                password,
                connectionPathPrefix,
                connectionRequestTimeout,
                connectionTimeout,
                socketTimeout,
                allowInsecure);
    }
}

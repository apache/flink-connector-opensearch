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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.opensearch.sink.Opensearch3Writer.DefaultFailureHandler;
import org.apache.flink.connector.opensearch.sink.Opensearch3Writer.Opensearch3FailureHandler;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import org.apache.hc.core5.http.HttpHost;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder to construct an OpenSearch 3.x compatible {@link Opensearch3Sink}.
 *
 * <p>The following example shows the minimal setup to create an Opensearch3Sink that submits
 * actions on checkpoint or the default number of actions was buffered (1000).
 *
 * <pre>{@code
 * Opensearch3Sink<String> sink = new Opensearch3SinkBuilder<String>()
 *     .setHosts(new HttpHost("localhost", 9200, "https"))
 *     .setEmitter((element, context, indexer) -> {
 *          Map<String, Object> doc = new HashMap<>();
 *          doc.put("data", element);
 *          indexer.addIndexRequest("my-index", element.hashCode() + "", doc);
 *      })
 *     .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
 *     .build();
 * }</pre>
 *
 * @param <IN> type of the records converted to OpenSearch actions
 */
@PublicEvolving
public class Opensearch3SinkBuilder<IN> {

    private int bulkFlushMaxActions = 1000;
    private int bulkFlushMaxMb = -1;
    private long bulkFlushInterval = -1;
    private FlushBackoffType bulkFlushBackoffType = FlushBackoffType.NONE;
    private int bulkFlushBackoffRetries = -1;
    private long bulkFlushBackOffDelay = -1;
    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    private List<HttpHost> hosts;
    protected Opensearch3Emitter<? super IN> emitter;
    private String username;
    private String password;
    private String connectionPathPrefix;
    private Integer connectionTimeout;
    private Integer connectionRequestTimeout;
    private Integer socketTimeout;
    private Boolean allowInsecure;
    private Opensearch3FailureHandler failureHandler = new DefaultFailureHandler();

    public Opensearch3SinkBuilder() {}

    @SuppressWarnings("unchecked")
    protected <S extends Opensearch3SinkBuilder<?>> S self() {
        return (S) this;
    }

    /**
     * Sets the emitter which is invoked on every record to convert it to OpenSearch operations.
     *
     * @param emitter to process records into OpenSearch operations.
     * @return this builder
     */
    public <T extends IN> Opensearch3SinkBuilder<T> setEmitter(
            Opensearch3Emitter<? super T> emitter) {
        checkNotNull(emitter);
        checkState(
                InstantiationUtil.isSerializable(emitter),
                "The OpenSearch emitter must be serializable.");

        final Opensearch3SinkBuilder<T> self = self();
        self.emitter = emitter;
        return self;
    }

    /**
     * Sets the hosts where the OpenSearch cluster nodes are reachable.
     *
     * @param hosts http addresses describing the node locations
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkState(hosts.length > 0, "Hosts cannot be empty.");
        for (HttpHost host : hosts) {
            checkNotNull(host, "Hosts cannot contain null elements.");
        }
        this.hosts = Arrays.asList(hosts);
        return self();
    }

    /**
     * Sets the wanted {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * DeliveryGuarantee#AT_LEAST_ONCE}
     *
     * @param deliveryGuarantee which describes the record emission behaviour
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        checkState(
                deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
                "OpenSearch sink does not support the EXACTLY_ONCE guarantee.");
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
        return self();
    }

    /**
     * Sets the maximum number of actions to buffer for each bulk request. You can pass -1 to
     * disable it. The default flush size is 1000.
     *
     * @param numMaxActions the maximum number of actions to buffer per bulk request.
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setBulkFlushMaxActions(int numMaxActions) {
        checkState(
                numMaxActions == -1 || numMaxActions > 0,
                "Max number of buffered actions must be larger than 0.");
        this.bulkFlushMaxActions = numMaxActions;
        return self();
    }

    /**
     * Sets the maximum size of buffered actions, in mb, per bulk request. You can pass -1 to
     * disable it.
     *
     * @param maxSizeMb the maximum size of buffered actions, in mb.
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setBulkFlushMaxSizeMb(int maxSizeMb) {
        checkState(
                maxSizeMb == -1 || maxSizeMb > 0,
                "Max size of buffered actions must be larger than 0.");
        this.bulkFlushMaxMb = maxSizeMb;
        return self();
    }

    /**
     * Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
     *
     * @param intervalMillis the bulk flush interval, in milliseconds.
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setBulkFlushInterval(long intervalMillis) {
        checkState(
                intervalMillis == -1 || intervalMillis >= 0,
                "Interval (in milliseconds) between each flush must be larger than "
                        + "or equal to 0.");
        this.bulkFlushInterval = intervalMillis;
        return self();
    }

    /**
     * Sets the type of back off to use when flushing bulk requests. The default bulk flush back off
     * type is {@link FlushBackoffType#NONE}.
     *
     * <p>Sets the amount of delay between each backoff attempt when flushing bulk requests, in
     * milliseconds.
     *
     * <p>Sets the maximum number of retries for a backoff attempt when flushing bulk requests.
     *
     * @param flushBackoffType the backoff type to use.
     * @param maxRetries the maximum number of retries.
     * @param delayMillis the delay between retries in milliseconds.
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setBulkFlushBackoffStrategy(
            FlushBackoffType flushBackoffType, int maxRetries, long delayMillis) {
        this.bulkFlushBackoffType = checkNotNull(flushBackoffType);
        checkState(
                flushBackoffType != FlushBackoffType.NONE,
                "FlushBackoffType#NONE does not require a configuration it is the default, "
                        + "retries and delay are ignored.");
        checkState(maxRetries > 0, "Max number of backoff attempts must be larger than 0.");
        this.bulkFlushBackoffRetries = maxRetries;
        checkState(
                delayMillis >= 0,
                "Delay (in milliseconds) between each backoff attempt must be larger "
                        + "than or equal to 0.");
        this.bulkFlushBackOffDelay = delayMillis;
        return self();
    }

    /**
     * Sets the username used to authenticate the connection with the OpenSearch cluster.
     *
     * @param username of the OpenSearch cluster user
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setConnectionUsername(String username) {
        checkNotNull(username);
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(username),
                "Username must not be empty or whitespace-only.");
        this.username = username;
        return self();
    }

    /**
     * Sets the password used to authenticate the connection with the OpenSearch cluster.
     *
     * @param password of the OpenSearch cluster user
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setConnectionPassword(String password) {
        checkNotNull(password);
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(password),
                "Password must not be empty or whitespace-only.");
        this.password = password;
        return self();
    }

    /**
     * Sets a prefix which used for every REST communication to the OpenSearch cluster.
     *
     * @param prefix for the communication
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setConnectionPathPrefix(String prefix) {
        checkNotNull(prefix);
        this.connectionPathPrefix = prefix;
        return self();
    }

    /**
     * Sets the timeout for requesting the connection of the OpenSearch cluster from the connection
     * manager.
     *
     * @param timeout for the connection request
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setConnectionRequestTimeout(int timeout) {
        checkState(timeout >= 0, "Connection request timeout must be larger than or equal to 0.");
        this.connectionRequestTimeout = timeout;
        return self();
    }

    /**
     * Sets the timeout for establishing a connection to the OpenSearch cluster.
     *
     * @param timeout for the connection
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setConnectionTimeout(int timeout) {
        checkState(timeout >= 0, "Connection timeout must be larger than or equal to 0.");
        this.connectionTimeout = timeout;
        return self();
    }

    /**
     * Sets the timeout for waiting for data or, put differently, a maximum period inactivity
     * between two consecutive data packets.
     *
     * @param timeout for the socket
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setSocketTimeout(int timeout) {
        checkState(timeout >= 0, "Socket timeout must be larger than or equal to 0.");
        this.socketTimeout = timeout;
        return self();
    }

    /**
     * Allows to bypass the certificates chain validation and connect to insecure network endpoints
     * (for example, servers which use self-signed certificates).
     *
     * @param allowInsecure allow or not to insecure network endpoints
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setAllowInsecure(boolean allowInsecure) {
        this.allowInsecure = allowInsecure;
        return self();
    }

    /**
     * Allows to set custom failure handler. If not set, the default failure handler will be used
     * which throws a runtime exception upon receiving a failure.
     *
     * @param failureHandler the custom handler
     * @return this builder
     */
    public Opensearch3SinkBuilder<IN> setFailureHandler(Opensearch3FailureHandler failureHandler) {
        checkNotNull(failureHandler);
        this.failureHandler = failureHandler;
        return self();
    }

    /**
     * Constructs the {@link Opensearch3Sink} with the properties configured this builder.
     *
     * @return {@link Opensearch3Sink}
     */
    public Opensearch3Sink<IN> build() {
        checkNotNull(emitter);
        checkNotNull(hosts);

        NetworkClientConfig networkClientConfig = buildNetworkClientConfig();
        BulkProcessorConfig bulkProcessorConfig = buildBulkProcessorConfig();

        return new Opensearch3Sink<>(
                hosts,
                emitter,
                deliveryGuarantee,
                bulkProcessorConfig,
                networkClientConfig,
                failureHandler);
    }

    private NetworkClientConfig buildNetworkClientConfig() {
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");

        return new NetworkClientConfig(
                username,
                password,
                connectionPathPrefix,
                connectionRequestTimeout,
                connectionTimeout,
                socketTimeout,
                allowInsecure);
    }

    private BulkProcessorConfig buildBulkProcessorConfig() {
        return new BulkProcessorConfig(
                bulkFlushMaxActions,
                bulkFlushMaxMb,
                bulkFlushInterval,
                bulkFlushBackoffType,
                bulkFlushBackoffRetries,
                bulkFlushBackOffDelay);
    }

    @Override
    public String toString() {
        return "Opensearch3SinkBuilder{"
                + "bulkFlushMaxActions="
                + bulkFlushMaxActions
                + ", bulkFlushMaxMb="
                + bulkFlushMaxMb
                + ", bulkFlushInterval="
                + bulkFlushInterval
                + ", bulkFlushBackoffType="
                + bulkFlushBackoffType
                + ", bulkFlushBackoffRetries="
                + bulkFlushBackoffRetries
                + ", bulkFlushBackOffDelay="
                + bulkFlushBackOffDelay
                + ", deliveryGuarantee="
                + deliveryGuarantee
                + ", hosts="
                + hosts
                + ", emitter="
                + emitter
                + ", username='"
                + username
                + '\''
                + ", password='"
                + password
                + '\''
                + ", connectionPathPrefix='"
                + connectionPathPrefix
                + '\''
                + ", allowInsecure='"
                + allowInsecure
                + '\''
                + '}';
    }
}

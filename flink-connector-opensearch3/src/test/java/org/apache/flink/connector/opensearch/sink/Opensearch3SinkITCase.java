/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.opensearch.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.opensearch.test.DockerImageVersions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.testcontainers.OpensearchContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link Opensearch3Sink}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class Opensearch3SinkITCase {
    protected static final Logger LOG = LoggerFactory.getLogger(Opensearch3SinkITCase.class);
    private static boolean failed;

    private Opensearch3TestClient context;

    @Container
    private static final OpensearchContainer OS_CONTAINER =
            new OpensearchContainer(DockerImageName.parse(DockerImageVersions.OPENSEARCH_3))
                    .withSecurityEnabled();

    @BeforeEach
    void setUp() throws Exception {
        failed = false;
        context = new Opensearch3TestClient(OS_CONTAINER);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (context != null) {
            context.close();
        }
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void testWriteToOpensearchWithDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee)
            throws Exception {
        final String index = "test-opensearch-with-delivery-" + deliveryGuarantee;
        boolean failure = false;
        try {
            runTest(index, false, TestEmitter::jsonEmitter, deliveryGuarantee, null);
        } catch (IllegalStateException e) {
            failure = true;
            assertThat(deliveryGuarantee).isSameAs(DeliveryGuarantee.EXACTLY_ONCE);
        } finally {
            assertThat(failure).isEqualTo(deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE);
        }
    }

    @ParameterizedTest
    @MethodSource("opensearchEmitters")
    void testWriteJsonToOpensearch(
            BiFunction<String, String, Opensearch3Emitter<Tuple2<Integer, String>>> emitterProvider)
            throws Exception {
        final String index = "test-opensearch-sink-" + UUID.randomUUID();
        runTest(index, false, emitterProvider, null);
    }

    @Test
    void testRecovery() throws Exception {
        final String index = "test-recovery-opensearch-sink";
        runTest(index, true, TestEmitter::jsonEmitter, new FailingMapper());
        assertThat(failed).isTrue();
    }

    private void runTest(
            String index,
            boolean allowRestarts,
            BiFunction<String, String, Opensearch3Emitter<Tuple2<Integer, String>>> emitterProvider,
            @Nullable MapFunction<Long, Long> additionalMapper)
            throws Exception {
        runTest(
                index,
                allowRestarts,
                emitterProvider,
                DeliveryGuarantee.AT_LEAST_ONCE,
                additionalMapper);
    }

    private void runTest(
            String index,
            boolean allowRestarts,
            BiFunction<String, String, Opensearch3Emitter<Tuple2<Integer, String>>> emitterProvider,
            DeliveryGuarantee deliveryGuarantee,
            @Nullable MapFunction<Long, Long> additionalMapper)
            throws Exception {
        final Opensearch3Sink<Tuple2<Integer, String>> sink =
                new Opensearch3SinkBuilder<Tuple2<Integer, String>>()
                        .setHosts(HttpHost.create(OS_CONTAINER.getHttpHostAddress()))
                        .setEmitter(emitterProvider.apply(index, context.getDataFieldName()))
                        .setBulkFlushMaxActions(5)
                        .setConnectionUsername(OS_CONTAINER.getUsername())
                        .setConnectionPassword(OS_CONTAINER.getPassword())
                        .setDeliveryGuarantee(deliveryGuarantee)
                        .setAllowInsecure(true)
                        .build();

        final Configuration configuration = new Configuration();
        if (!allowRestarts) {
            configuration.set(
                    RestartStrategyOptions.RESTART_STRATEGY,
                    RestartStrategyType.NO_RESTART_STRATEGY.getMainValue());
        }
        final StreamExecutionEnvironment env = new LocalStreamEnvironment(configuration);
        // Recovery test: shorter interval so a checkpoint can complete while fromSequence(1,5) is
        // still running (FailingMapper fails from notifyCheckpointComplete).
        env.enableCheckpointing(additionalMapper != null ? 20L : 100L);
        // Increase checkpoint timeout for slower container environments (e.g., Podman)
        env.getCheckpointConfig().setCheckpointTimeout(300_000L); // 5 minutes
        DataStream<Long> stream = env.fromSequence(1, 5);

        if (additionalMapper != null) {
            stream = stream.map(additionalMapper);
        }

        stream.map(
                        new MapFunction<Long, Tuple2<Integer, String>>() {
                            @Override
                            public Tuple2<Integer, String> map(Long value) throws Exception {
                                return Tuple2.of(
                                        value.intValue(),
                                        Opensearch3TestClient.buildMessage(value.intValue()));
                            }
                        })
                .sinkTo(sink);
        env.execute();
        context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5);
    }

    private static List<BiFunction<String, String, Opensearch3Emitter<Tuple2<Integer, String>>>>
            opensearchEmitters() {
        return Arrays.asList(TestEmitter::jsonEmitter);
    }

    private static class FailingMapper implements MapFunction<Long, Long>, CheckpointListener {

        private int emittedRecords = 0;

        @Override
        public Long map(Long value) throws Exception {
            emittedRecords++;
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (failed || emittedRecords == 0) {
                return;
            }
            failed = true;
            throw new Exception("Expected failure");
        }
    }

    /** Test emitter for OpenSearch 3.x. */
    static class TestEmitter implements Opensearch3Emitter<Tuple2<Integer, String>> {

        private final String index;
        private final String dataFieldName;

        TestEmitter(String index, String dataFieldName) {
            this.index = index;
            this.dataFieldName = dataFieldName;
        }

        public static Opensearch3Emitter<Tuple2<Integer, String>> jsonEmitter(
                String index, String dataFieldName) {
            return new TestEmitter(index, dataFieldName);
        }

        @Override
        public void emit(
                Tuple2<Integer, String> element,
                org.apache.flink.api.connector.sink2.SinkWriter.Context context,
                Opensearch3RequestIndexer indexer) {
            Map<String, Object> document = new HashMap<>();
            document.put(dataFieldName, element.f1);
            indexer.addIndexRequest(index, element.f0.toString(), document);
        }
    }
}

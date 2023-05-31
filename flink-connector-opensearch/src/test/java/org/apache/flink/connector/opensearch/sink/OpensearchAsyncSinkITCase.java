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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.opensearch.OpensearchUtil;
import org.apache.flink.connector.opensearch.test.DockerImageVersions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.testcontainers.OpensearchContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OpensearchAsyncSink}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class OpensearchAsyncSinkITCase {
    protected static final Logger LOG = LoggerFactory.getLogger(OpensearchAsyncSinkITCase.class);
    private static boolean failed;

    private RestHighLevelClient client;
    private OpensearchTestClient context;

    @Container
    private static final OpensearchContainer OS_CONTAINER =
            OpensearchUtil.createOpensearchContainer(DockerImageVersions.OPENSEARCH_1, LOG);

    @BeforeEach
    void setUp() {
        failed = false;
        client = OpensearchUtil.createClient(OS_CONTAINER);
        context = new OpensearchTestClient(client);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @ParameterizedTest
    @MethodSource("opensearchConverters")
    void testWriteJsonToOpensearch(
            BiFunction<
                            String,
                            String,
                            ElementConverter<Tuple2<Integer, String>, DocWriteRequest<?>>>
                    converterProvider)
            throws Exception {
        final String index = "test-opensearch-async-sink-" + UUID.randomUUID();
        runTest(index, false, converterProvider, null);
    }

    @Test
    void testRecovery() throws Exception {
        final String index = "test-recovery-opensearch-async-sink";
        runTest(index, true, TestConverter::jsonConverter, new FailingMapper());
        assertThat(failed).isTrue();
    }

    private void runTest(
            String index,
            boolean allowRestarts,
            BiFunction<
                            String,
                            String,
                            ElementConverter<Tuple2<Integer, String>, DocWriteRequest<?>>>
                    converterProvider,
            @Nullable MapFunction<Long, Long> additionalMapper)
            throws Exception {
        final OpensearchAsyncSinkBuilder<Tuple2<Integer, String>> builder =
                OpensearchAsyncSink.<Tuple2<Integer, String>>builder()
                        .setHosts(HttpHost.create(OS_CONTAINER.getHttpHostAddress()))
                        .setElementConverter(
                                converterProvider.apply(index, context.getDataFieldName()))
                        .setMaxBatchSize(5)
                        .setConnectionUsername(OS_CONTAINER.getUsername())
                        .setConnectionPassword(OS_CONTAINER.getPassword())
                        .setAllowInsecure(true);

        try (final StreamExecutionEnvironment env = new LocalStreamEnvironment()) {
            env.enableCheckpointing(100L);
            if (!allowRestarts) {
                env.setRestartStrategy(RestartStrategies.noRestart());
            }
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
                                            OpensearchTestClient.buildMessage(value.intValue()));
                                }
                            })
                    .sinkTo(builder.build());
            env.execute();
            context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5);
        }
    }

    private static List<
                    BiFunction<
                            String,
                            String,
                            ElementConverter<Tuple2<Integer, String>, DocWriteRequest<?>>>>
            opensearchConverters() {
        return Arrays.asList(TestConverter::jsonConverter, TestConverter::smileConverter);
    }

    private static class FailingMapper implements MapFunction<Long, Long>, CheckpointListener {
        private static final long serialVersionUID = 1L;
        private int emittedRecords = 0;

        @Override
        public Long map(Long value) throws Exception {
            Thread.sleep(50);
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
}

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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.opensearch.OpensearchUtil;
import org.apache.flink.connector.opensearch.test.DockerImageVersions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.testcontainers.OpensearchContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.apache.flink.connector.opensearch.sink.OpensearchTestClient.buildMessage;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OpensearchAsyncWriter}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class OpensearchAsyncWriterITCase {

    private static final Logger LOG = LoggerFactory.getLogger(OpensearchAsyncWriterITCase.class);

    @Container
    private static final OpensearchContainer OS_CONTAINER =
            OpensearchUtil.createOpensearchContainer(DockerImageVersions.OPENSEARCH_1, LOG);

    private RestHighLevelClient client;
    private OpensearchTestClient clientContext;
    private TestSinkInitContext context;

    private final Lock lock = new ReentrantLock();
    private final Condition completed = lock.newCondition();
    private final List<DocSerdeRequest> requests = new ArrayList<>();

    @BeforeEach
    void setUp() {
        client = OpensearchUtil.createClient(OS_CONTAINER);
        clientContext = new OpensearchTestClient(client);
        context = new TestSinkInitContext();
        requests.clear();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @Timeout(5)
    void testWriteOnBulkFlush() throws Exception {
        final String index = "test-bulk-flush-without-checkpoint-async";
        final int maxBatchSize = 5;

        try (final OpensearchAsyncWriter<Tuple2<Integer, String>> writer =
                createWriter(context, index, maxBatchSize, Long.MAX_VALUE)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);

            // Ignore flush on checkpoint
            writer.flush(false);
            clientContext.assertThatIdsAreNotWritten(index, 1, 2, 3, 4);

            // Trigger flush
            writer.write(Tuple2.of(5, "test-5"), null);

            /* await for async bulk request to complete */
            awaitForCompletion();

            clientContext.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5);

            writer.write(Tuple2.of(6, "test-6"), null);
            clientContext.assertThatIdsAreNotWritten(index, 6);

            // Force flush
            writer.flush(true);
            clientContext.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5, 6);
        }
    }

    @Test
    @Timeout(5)
    void testWriteOnBulkIntervalFlush() throws Exception {
        final String index = "test-bulk-flush-with-interval-async";

        try (final OpensearchAsyncWriter<Tuple2<Integer, String>> writer =
                createWriter(context, index, 10, 1000 /* 1s */)) {

            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);

            /* advance timer */
            context.getTestProcessingTimeService().advance(1200);

            /* await for async bulk request to complete */
            awaitForCompletion();
        }

        clientContext.assertThatIdsAreWritten(index, 1, 2, 3, 4);
    }

    @Test
    void testIncrementByteOutMetric() throws Exception {
        final String index = "test-inc-byte-out-async";
        final int flushAfterNActions = 2;

        try (final OpensearchAsyncWriter<Tuple2<Integer, String>> writer =
                createWriter(context, index, flushAfterNActions, Long.MAX_VALUE)) {
            final Counter numBytesOut = context.getNumBytesOutCounter();
            assertThat(numBytesOut.getCount()).isEqualTo(0);
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.flush(true);
            long first = numBytesOut.getCount();

            assertThat(first).isGreaterThan(0);

            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.flush(true);
            assertThat(numBytesOut.getCount()).isGreaterThan(first);
        }
    }

    @Test
    void testIncrementRecordsSendMetric() throws Exception {
        final String index = "test-inc-records-send-async";
        final int flushAfterNActions = 2;

        try (final OpensearchAsyncWriter<Tuple2<Integer, String>> writer =
                createWriter(context, index, flushAfterNActions, Long.MAX_VALUE)) {
            final Counter recordsSend = context.getNumRecordsOutCounter();

            writer.write(Tuple2.of(1, buildMessage(1)), null);
            // Update existing index
            writer.write(Tuple2.of(1, "u" + buildMessage(2)), null);
            // Delete index
            writer.write(Tuple2.of(1, "d" + buildMessage(3)), null);

            writer.flush(true);

            assertThat(recordsSend.getCount()).isEqualTo(3L);
        }
    }

    @Test
    void testCurrentSendTime() throws Exception {
        final String index = "test-current-send-time-async";
        final int flushAfterNActions = 2;

        try (final OpensearchAsyncWriter<Tuple2<Integer, String>> writer =
                createWriter(context, index, flushAfterNActions, Long.MAX_VALUE)) {
            final Optional<Gauge<Long>> currentSendTime = context.getCurrentSendTimeGauge();

            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.flush(true);

            assertThat(currentSendTime).isPresent();
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }
    }

    @Test
    @Timeout(5)
    void testWriteError() throws Exception {
        final String index = "test-bulk-flush-error-async";
        final int maxBatchSize = 5;

        try (final OpensearchAsyncWriter<Tuple2<Integer, String>> writer =
                createWriter(context, index, maxBatchSize, Long.MAX_VALUE)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);

            // Force flush
            writer.flush(true);
            clientContext.assertThatIdsAreWritten(index, 1);
            assertThat(requests).hasSize(0);

            // The "c" prefix should force the create mode and fail the bulk item request
            // (duplicate)
            writer.write(Tuple2.of(1, buildMessage(1, "c")), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);
            writer.write(Tuple2.of(5, buildMessage(5)), null);

            /* await for async bulk request to complete */
            awaitForCompletion();

            // Force flush
            clientContext.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5);
            assertThat(requests).hasSize(1);
        }
    }

    private OpensearchAsyncWriter<Tuple2<Integer, String>> createWriter(
            Sink.InitContext context, String index, int maxBatchSize, long maxTimeInBufferMS) {
        return new OpensearchAsyncWriter<Tuple2<Integer, String>>(
                context,
                new UpdatingElementConverter(index, clientContext.getDataFieldName()),
                maxBatchSize,
                1,
                100,
                Long.MAX_VALUE,
                maxTimeInBufferMS,
                Long.MAX_VALUE,
                Collections.singletonList(HttpHost.create(OS_CONTAINER.getHttpHostAddress())),
                new NetworkClientConfig(
                        OS_CONTAINER.getUsername(),
                        OS_CONTAINER.getPassword(),
                        null,
                        null,
                        null,
                        null,
                        true),
                Collections.emptyList()) {
            @Override
            protected void submitRequestEntries(
                    List<DocSerdeRequest> requestEntries,
                    Consumer<List<DocSerdeRequest>> requestResult) {
                super.submitRequestEntries(
                        requestEntries,
                        (entries) -> {
                            requestResult.accept(entries);

                            lock.lock();
                            try {
                                requests.addAll(entries);
                                completed.signal();
                            } finally {
                                lock.unlock();
                            }
                        });
            }
        };
    }

    private static class UpdatingElementConverter
            implements ElementConverter<Tuple2<Integer, String>, DocSerdeRequest> {
        private static final long serialVersionUID = 1L;

        private final String dataFieldName;
        private final String index;

        UpdatingElementConverter(String index, String dataFieldName) {
            this.index = index;
            this.dataFieldName = dataFieldName;
        }

        @Override
        public DocSerdeRequest apply(Tuple2<Integer, String> element, Context context) {
            Map<String, Object> document = new HashMap<>();
            document.put(dataFieldName, element.f1);

            final char action = element.f1.charAt(0);
            final String id = element.f0.toString();
            switch (action) {
                case 'c':
                    return DocSerdeRequest.from(
                            new IndexRequest(index).create(true).id(id).source(document));
                case 'd':
                    return DocSerdeRequest.from(new DeleteRequest(index).id(id));
                case 'u':
                    return DocSerdeRequest.from(
                            new UpdateRequest().index(index).id(id).doc(document));
                default:
                    return DocSerdeRequest.from(new IndexRequest(index).id(id).source(document));
            }
        }
    }

    private void awaitForCompletion() throws InterruptedException {
        lock.lock();
        try {
            completed.await();
        } finally {
            lock.unlock();
        }
    }
}

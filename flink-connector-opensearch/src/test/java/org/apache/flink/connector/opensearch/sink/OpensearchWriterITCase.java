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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.opensearch.OpensearchUtil;
import org.apache.flink.connector.opensearch.test.DockerImageVersions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.connector.opensearch.sink.OpensearchTestClient.buildMessage;
import static org.apache.flink.connector.opensearch.sink.OpensearchWriter.DEFAULT_FAILURE_HANDLER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OpensearchWriter}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class OpensearchWriterITCase {

    private static final Logger LOG = LoggerFactory.getLogger(OpensearchWriterITCase.class);

    @Container
    private static final OpensearchContainer OS_CONTAINER =
            OpensearchUtil.createOpensearchContainer(DockerImageVersions.OPENSEARCH_1, LOG);

    private RestHighLevelClient client;
    private OpensearchTestClient context;
    private MetricListener metricListener;

    @BeforeEach
    void setUp() {
        metricListener = new MetricListener();
        client = OpensearchUtil.createClient(OS_CONTAINER);
        context = new OpensearchTestClient(client);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testWriteOnBulkFlush() throws Exception {
        final String index = "test-bulk-flush-without-checkpoint";
        final int flushAfterNActions = 5;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final OpensearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);

            // Ignore flush on checkpoint
            writer.flush(false);

            context.assertThatIdsAreNotWritten(index, 1, 2, 3, 4);

            // Trigger flush
            writer.write(Tuple2.of(5, "test-5"), null);
            context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5);

            writer.write(Tuple2.of(6, "test-6"), null);
            context.assertThatIdsAreNotWritten(index, 6);

            // Force flush
            writer.blockingFlushAllActions();
            context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5, 6);
        }
    }

    @Test
    void testWriteOnBulkIntervalFlush() throws Exception {
        final String index = "test-bulk-flush-with-interval";

        // Configure bulk processor to flush every 1s;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(-1, -1, 1000, FlushBackoffType.NONE, 0, 0);

        try (final OpensearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);
            writer.blockingFlushAllActions();
        }

        context.assertThatIdsAreWritten(index, 1, 2, 3, 4);
    }

    @Test
    void testWriteOnCheckpoint() throws Exception {
        final String index = "test-bulk-flush-with-checkpoint";
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(-1, -1, -1, FlushBackoffType.NONE, 0, 0);

        // Enable flush on checkpoint
        try (final OpensearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, true, bulkProcessorConfig)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);

            context.assertThatIdsAreNotWritten(index, 1, 2, 3);

            // Trigger flush
            writer.flush(false);

            context.assertThatIdsAreWritten(index, 1, 2, 3);
        }
    }

    @Test
    void testIncrementByteOutMetric() throws Exception {
        final String index = "test-inc-byte-out";
        final OperatorIOMetricGroup operatorIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(
                        metricListener.getMetricGroup(), operatorIOMetricGroup);
        final int flushAfterNActions = 2;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final OpensearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, false, bulkProcessorConfig, metricGroup)) {
            final Counter numBytesOut = operatorIOMetricGroup.getNumBytesOutCounter();
            assertThat(numBytesOut.getCount()).isEqualTo(0);
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.blockingFlushAllActions();
            long first = numBytesOut.getCount();

            assertThat(first).isGreaterThan(0);

            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.blockingFlushAllActions();
            assertThat(numBytesOut.getCount()).isGreaterThan(first);
        }
    }

    @Test
    void testIncrementRecordsSendMetric() throws Exception {
        final String index = "test-inc-records-send";
        final int flushAfterNActions = 2;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final OpensearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            final Optional<Counter> recordsSend =
                    metricListener.getCounter(MetricNames.NUM_RECORDS_SEND);
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            // Update existing index
            writer.write(Tuple2.of(1, "u" + buildMessage(2)), null);
            // Delete index
            writer.write(Tuple2.of(1, "d" + buildMessage(3)), null);

            writer.blockingFlushAllActions();

            assertThat(recordsSend).isPresent();
            assertThat(recordsSend.get().getCount()).isEqualTo(3L);
        }
    }

    @Test
    void testCurrentSendTime() throws Exception {
        final String index = "test-current-send-time";
        final int flushAfterNActions = 2;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final OpensearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            final Optional<Gauge<Long>> currentSendTime =
                    metricListener.getGauge("currentSendTime");
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.blockingFlushAllActions();

            assertThat(currentSendTime).isPresent();
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }
    }

    private static class TestHandler implements FailureHandler {
        private boolean failed = false;

        private synchronized void setFailed() {
            failed = true;
        }

        public boolean isFailed() {
            return failed;
        }

        @Override
        public void onFailure(Throwable failure) {
            setFailed();
        }
    }

    @Test
    void testWriteErrorOnUpdate() throws Exception {
        final String index = "test-bulk-flush-with-error";
        final int flushAfterNActions = 1;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        final TestHandler testHandler = new TestHandler();
        try (final OpensearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, true, bulkProcessorConfig, testHandler)) {
            // Trigger an error by updating non-existing document
            writer.write(Tuple2.of(1, "u" + buildMessage(1)), null);
            context.assertThatIdsAreNotWritten(index, 1);
            assertThat(testHandler.isFailed()).isEqualTo(true);
        }
    }

    private OpensearchWriter<Tuple2<Integer, String>> createWriter(
            String index, boolean flushOnCheckpoint, BulkProcessorConfig bulkProcessorConfig) {
        return createWriter(
                index,
                flushOnCheckpoint,
                bulkProcessorConfig,
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup()),
                DEFAULT_FAILURE_HANDLER);
    }

    private OpensearchWriter<Tuple2<Integer, String>> createWriter(
            String index,
            boolean flushOnCheckpoint,
            BulkProcessorConfig bulkProcessorConfig,
            FailureHandler failureHandler) {
        return createWriter(
                index,
                flushOnCheckpoint,
                bulkProcessorConfig,
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup()),
                failureHandler);
    }

    private OpensearchWriter<Tuple2<Integer, String>> createWriter(
            String index,
            boolean flushOnCheckpoint,
            BulkProcessorConfig bulkProcessorConfig,
            SinkWriterMetricGroup metricGroup) {
        return createWriter(
                index,
                flushOnCheckpoint,
                bulkProcessorConfig,
                metricGroup,
                DEFAULT_FAILURE_HANDLER);
    }

    private OpensearchWriter<Tuple2<Integer, String>> createWriter(
            String index,
            boolean flushOnCheckpoint,
            BulkProcessorConfig bulkProcessorConfig,
            SinkWriterMetricGroup metricGroup,
            FailureHandler failureHandler) {
        return new OpensearchWriter<Tuple2<Integer, String>>(
                Collections.singletonList(HttpHost.create(OS_CONTAINER.getHttpHostAddress())),
                new UpdatingEmitter(index, context.getDataFieldName()),
                flushOnCheckpoint,
                bulkProcessorConfig,
                new NetworkClientConfig(
                        OS_CONTAINER.getUsername(),
                        OS_CONTAINER.getPassword(),
                        null,
                        null,
                        null,
                        null,
                        true),
                metricGroup,
                new TestMailbox(),
                new DefaultRestClientFactory(),
                DEFAULT_FAILURE_HANDLER);
    }

    private static class UpdatingEmitter implements OpensearchEmitter<Tuple2<Integer, String>> {
        private static final long serialVersionUID = 1L;

        private final String dataFieldName;
        private final String index;

        UpdatingEmitter(String index, String dataFieldName) {
            this.index = index;
            this.dataFieldName = dataFieldName;
        }

        @Override
        public void emit(
                Tuple2<Integer, String> element,
                SinkWriter.Context context,
                RequestIndexer indexer) {

            Map<String, Object> document = new HashMap<>();
            document.put(dataFieldName, element.f1);

            final char action = element.f1.charAt(0);
            final String id = element.f0.toString();
            switch (action) {
                case 'd':
                    {
                        indexer.add(new DeleteRequest(index).id(id));
                        break;
                    }
                case 'u':
                    {
                        indexer.add(new UpdateRequest().index(index).id(id).doc(document));
                        break;
                    }
                default:
                    {
                        indexer.add(new IndexRequest(index).id(id).source(document));
                    }
            }
        }
    }

    private static class TestMailbox implements MailboxExecutor {

        @Override
        public void execute(
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            try {
                command.run();
            } catch (Exception e) {
                throw new RuntimeException("Unexpected error", e);
            }
        }

        @Override
        public void yield() throws InterruptedException, FlinkRuntimeException {
            Thread.sleep(100);
        }

        @Override
        public boolean tryYield() throws FlinkRuntimeException {
            return false;
        }
    }
}

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

package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.opensearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.protocol.HttpRequestHandlerMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkItemResponse.Failure;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Requests;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.shard.ShardId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Suite of tests for {@link OpensearchSink}. */
public class OpensearchSinkTest {
    private HttpServer server;
    private final Deque<Consumer<HttpResponse>> responses = new ConcurrentLinkedDeque<>();
    private final Lock lock = new ReentrantLock();
    private final Condition flushed = lock.newCondition();

    @BeforeEach
    public void setUp() throws IOException {
        final HttpRequestHandlerMapper handlers =
                (request) -> {
                    final String method = request.getRequestLine().getMethod();
                    if (method.equalsIgnoreCase("HEAD")) {
                        // Connection request always OKed
                        return (req, resp, context) -> resp.setStatusCode(200);
                    } else if (method.equalsIgnoreCase("POST")) {
                        // Bulk responses are configured per test case
                        return (req, resp, context) -> {
                            lock.lock();
                            try {
                                responses.poll().accept(resp);
                                flushed.signalAll();
                            } finally {
                                lock.unlock();
                            }
                        };
                    } else {
                        return null;
                    }
                };
        server = ServerBootstrap.bootstrap().setHandlerMapper(handlers).create();
        server.start();
    }

    @AfterEach
    public void tearDown() {
        server.stop();
        server = null;
        responses.clear();
    }

    /**
     * Tests that any item failure in the listener callbacks is rethrown on an immediately following
     * invoke call.
     */
    @Test
    public void testItemFailureRethrownOnInvoke() throws Throwable {
        final OpensearchSink.Builder<String> builder =
                new OpensearchSink.Builder<>(
                        Arrays.asList(new HttpHost("localhost", server.getLocalPort())),
                        new SimpleSinkFunction<String>());
        builder.setBulkFlushMaxActions(1);
        builder.setFailureHandler(new NoOpFailureHandler());

        final OpensearchSink<String> sink = builder.build();
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        responses.add(
                createResponse(
                        new BulkItemResponse(
                                1,
                                OpType.INDEX,
                                new Failure(
                                        "test",
                                        "_doc",
                                        "1",
                                        new Exception("artificial failure for record")))));
        testHarness.open();

        // setup the next bulk request, and its mock item failures
        testHarness.processElement(new StreamRecord<>("msg"));

        assertThatThrownBy(() -> testHarness.processElement(new StreamRecord<>("next msg")))
                .getCause()
                .hasMessageContaining("artificial failure for record");
    }

    /**
     * Tests that any item failure in the listener callbacks is rethrown on an immediately following
     * checkpoint.
     */
    @Test
    public void testItemFailureRethrownOnCheckpoint() throws Throwable {
        final OpensearchSink.Builder<String> builder =
                new OpensearchSink.Builder<>(
                        Arrays.asList(new HttpHost("localhost", server.getLocalPort())),
                        new SimpleSinkFunction<String>());
        builder.setBulkFlushMaxActions(1);
        builder.setFailureHandler(new NoOpFailureHandler());

        final OpensearchSink<String> sink = builder.build();
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        responses.add(
                createResponse(
                        new BulkItemResponse(
                                1,
                                OpType.INDEX,
                                new Failure(
                                        "test",
                                        "_doc",
                                        "1",
                                        new Exception("artificial failure for record")))));
        testHarness.processElement(new StreamRecord<>("msg"));

        assertThatThrownBy(() -> testHarness.snapshot(1L, 1000L))
                .getCause()
                .getCause()
                .hasMessageContaining("artificial failure for record");
    }

    /**
     * Tests that any item failure in the listener callbacks due to flushing on an immediately
     * following checkpoint is rethrown; we set a timeout because the test will not finish if the
     * logic is broken.
     */
    @Test
    @Timeout(5)
    public void testItemFailureRethrownOnCheckpointAfterFlush() throws Throwable {
        final OpensearchSink.Builder<String> builder =
                new OpensearchSink.Builder<>(
                        Arrays.asList(new HttpHost("localhost", server.getLocalPort())),
                        new SimpleSinkFunction<String>());
        builder.setBulkFlushInterval(1000);
        builder.setFailureHandler(new NoOpFailureHandler());

        final OpensearchSink<String> sink = builder.build();
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        responses.add(
                createResponse(
                        new BulkItemResponse(
                                1,
                                OpType.INDEX,
                                new IndexResponse(
                                        new ShardId("test", "-", 0), "_doc", "1", 0, 0, 1, true))));

        responses.add(
                createResponse(
                        new BulkItemResponse(
                                2,
                                OpType.INDEX,
                                new Failure(
                                        "test",
                                        "_doc",
                                        "2",
                                        new Exception("artificial failure for record")))));

        testHarness.processElement(new StreamRecord<>("msg-1"));

        // Await for flush to be complete
        awaitForFlushToFinish();

        // setup the requests to be flushed in the snapshot
        testHarness.processElement(new StreamRecord<>("msg-2"));
        // let the snapshot-triggered flush continue (2 records in the bulk, so the 2nd one should
        // fail)
        testHarness.processElement(new StreamRecord<>("msg-3"));

        CheckedThread snapshotThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.snapshot(1L, 1000L);
                    }
                };
        snapshotThread.start();

        // Await for flush to be complete
        awaitForFlushToFinish();

        assertThatThrownBy(snapshotThread::sync)
                .getCause()
                .getCause()
                .hasMessageContaining("artificial failure for record");
    }

    /**
     * Tests that any bulk failure in the listener callbacks is rethrown on an immediately following
     * invoke call.
     */
    @Test
    public void testBulkFailureRethrownOnInvoke() throws Throwable {
        final OpensearchSink.Builder<String> builder =
                new OpensearchSink.Builder<>(
                        Arrays.asList(new HttpHost("localhost", server.getLocalPort())),
                        new SimpleSinkFunction<String>());
        builder.setBulkFlushMaxActions(1);
        builder.setFailureHandler(new NoOpFailureHandler());

        final OpensearchSink<String> sink = builder.build();
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        // Let the whole bulk request fail
        responses.add(response -> response.setStatusCode(500));

        testHarness.processElement(new StreamRecord<>("msg"));

        assertThatThrownBy(() -> testHarness.processElement(new StreamRecord<>("next msg")))
                .getCause()
                .hasMessageContaining("Unable to parse response body");
    }

    /**
     * Tests that any bulk failure in the listener callbacks is rethrown on an immediately following
     * checkpoint.
     */
    @Test
    public void testBulkFailureRethrownOnCheckpoint() throws Throwable {
        final OpensearchSink.Builder<String> builder =
                new OpensearchSink.Builder<>(
                        Arrays.asList(new HttpHost("localhost", server.getLocalPort())),
                        new SimpleSinkFunction<String>());
        builder.setBulkFlushMaxActions(1);
        builder.setFailureHandler(new NoOpFailureHandler());

        final OpensearchSink<String> sink = builder.build();
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        // Let the whole bulk request fail
        responses.add(response -> response.setStatusCode(500));

        testHarness.processElement(new StreamRecord<>("msg"));

        assertThatThrownBy(() -> testHarness.snapshot(1L, 1000L))
                .getCause()
                .getCause()
                .hasMessageContaining("Unable to parse response body");
    }

    /**
     * Tests that any bulk failure in the listener callbacks due to flushing on an immediately
     * following checkpoint is rethrown; we set a timeout because the test will not finish if the
     * logic is broken.
     */
    @Test
    @Timeout(5)
    public void testBulkFailureRethrownOnOnCheckpointAfterFlush() throws Throwable {
        final OpensearchSink.Builder<String> builder =
                new OpensearchSink.Builder<>(
                        Arrays.asList(new HttpHost("localhost", server.getLocalPort())),
                        new SimpleSinkFunction<String>());
        builder.setBulkFlushInterval(500);
        builder.setFailureHandler(new NoOpFailureHandler());

        final OpensearchSink<String> sink = builder.build();
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        responses.add(
                createResponse(
                        new BulkItemResponse(
                                1,
                                OpType.INDEX,
                                new IndexResponse(
                                        new ShardId("test", "-", 0), "_doc", "1", 0, 0, 1, true))));

        // Let the whole bulk request fail
        responses.add(response -> response.setStatusCode(500));

        // setup the next bulk request, and let bulk request succeed
        testHarness.processElement(new StreamRecord<>("msg-1"));

        // Await for flush to be complete
        awaitForFlushToFinish();

        // setup the requests to be flushed in the snapshot
        testHarness.processElement(new StreamRecord<>("msg-2"));
        testHarness.processElement(new StreamRecord<>("msg-3"));

        CheckedThread snapshotThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.snapshot(1L, 1000L);
                    }
                };
        snapshotThread.start();

        // Await for flush to be complete
        awaitForFlushToFinish();

        assertThatThrownBy(snapshotThread::sync)
                .getCause()
                .getCause()
                .hasMessageContaining("Unable to parse response body");
    }

    /**
     * Tests that the sink correctly waits for pending requests (including re-added requests) on
     * checkpoints; we set a timeout because the test will not finish if the logic is broken.
     */
    @Test
    @Timeout(5)
    public void testAtLeastOnceSink() throws Throwable {
        final OpensearchSink.Builder<String> builder =
                new OpensearchSink.Builder<>(
                        Arrays.asList(new HttpHost("localhost", server.getLocalPort())),
                        new SimpleSinkFunction<String>());
        builder.setBulkFlushInterval(500);
        // use a failure handler that simply re-adds requests
        builder.setFailureHandler(new DummyRetryFailureHandler());

        final OpensearchSink<String> sink = builder.build();
        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        // setup the next bulk request, and its mock item failures;
        // it contains 1 request, which will fail and re-added to the next bulk request
        responses.add(
                createResponse(
                        new BulkItemResponse(
                                1,
                                OpType.INDEX,
                                new Failure(
                                        "test",
                                        "_doc",
                                        "1",
                                        new Exception("artificial failure for record")))));

        responses.add(
                createResponse(
                        new BulkItemResponse(
                                2,
                                OpType.INDEX,
                                new IndexResponse(
                                        new ShardId("test", "-", 0), "_doc", "2", 0, 0, 1, true))));

        testHarness.processElement(new StreamRecord<>("msg"));

        // current number of pending request should be 1 due to the re-add
        assertThat(sink.getNumPendingRequests()).isEqualTo(1);

        CheckedThread snapshotThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        testHarness.snapshot(1L, 1000L);
                    }
                };
        snapshotThread.start();

        // Await for flush to be complete
        awaitForFlushToFinish();

        // since the previous flush should have resulted in a request re-add from the failure
        // handler,
        // we should have flushed again, and eventually be blocked before snapshot triggers the 2nd
        // flush

        // current number of pending request should be 1 due to the re-add, since the
        // failureRequestIndexer will be called only on the next bulk flush interval, we may need
        // to wait for numPendingRequests to be updated.
        awaitForCondition(() -> sink.getNumPendingRequests() == 1);

        // Await for flush to be complete
        awaitForFlushToFinish();

        // the snapshot should finish with no exceptions
        snapshotThread.sync();

        testHarness.close();
    }

    /**
     * This test is meant to assure that testAtLeastOnceSink is valid by testing that if flushing is
     * disabled, the snapshot method does indeed finishes without waiting for pending requests; we
     * set a timeout because the test will not finish if the logic is broken.
     */
    @Test
    @Timeout(5)
    public void testDoesNotWaitForPendingRequestsIfFlushingDisabled() throws Exception {
        final OpensearchSink.Builder<String> builder =
                new OpensearchSink.Builder<>(
                        Arrays.asList(new HttpHost("localhost", server.getLocalPort())),
                        new SimpleSinkFunction<String>());

        final OpensearchSink<String> sink = builder.build();
        sink.disableFlushOnCheckpoint(); // disable flushing

        final OneInputStreamOperatorTestHarness<String, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();

        responses.add(
                createResponse(
                        new BulkItemResponse(
                                1,
                                OpType.INDEX,
                                new Failure(
                                        "test",
                                        "_doc",
                                        "1",
                                        new Exception("artificial failure for record")))));

        testHarness.processElement(new StreamRecord<>("msg-1"));

        // the snapshot should not block even though we haven't flushed the bulk request
        testHarness.snapshot(1L, 1000L);

        assertThatThrownBy(() -> testHarness.close())
                .getCause()
                .hasMessageContaining("artificial failure for record");
    }

    @Test
    public void testOpenAndCloseInSinkFunction() throws Exception {
        final SimpleClosableSinkFunction<String> sinkFunction = new SimpleClosableSinkFunction<>();
        final OpensearchSink.Builder<String> builder =
                new OpensearchSink.Builder<>(
                        Arrays.asList(new HttpHost("localhost", server.getLocalPort())),
                        sinkFunction);
        builder.setFailureHandler(new DummyRetryFailureHandler());

        final OpensearchSink<String> sink = builder.build();
        sink.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
        sink.open(new Configuration());
        sink.close();

        assertThat(sinkFunction.openCalled).isTrue();
        assertThat(sinkFunction.closeCalled).isTrue();
    }

    private static class SimpleSinkFunction<String> implements OpensearchSinkFunction<String> {
        private static final long serialVersionUID = -176739293659135148L;

        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
            Map<java.lang.String, Object> json = new HashMap<>();
            json.put("data", element);

            indexer.add(Requests.indexRequest().index("index").type("type").id("id").source(json));
        }
    }

    private static class SimpleClosableSinkFunction<String>
            implements OpensearchSinkFunction<String> {

        private static final long serialVersionUID = 1872065917794006848L;

        private boolean openCalled;
        private boolean closeCalled;

        @Override
        public void open() {
            openCalled = true;
        }

        @Override
        public void close() {
            closeCalled = true;
        }

        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {}
    }

    private static class DummyRetryFailureHandler implements ActionRequestFailureHandler {
        private static final long serialVersionUID = 5400023700099200745L;

        @Override
        public void onFailure(
                ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer)
                throws Throwable {
            indexer.add(action);
        }
    }

    private static Consumer<HttpResponse> createResponse(BulkItemResponse item) {
        return response -> {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                response.setStatusCode(200);
                try (XContentBuilder builder =
                        new XContentBuilder(JsonXContent.jsonXContent, baos)) {
                    final BulkResponse bulkResponse =
                            new BulkResponse(new BulkItemResponse[] {item}, 200);
                    bulkResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
                }
                response.setEntity(
                        new ByteArrayEntity(baos.toByteArray(), ContentType.APPLICATION_JSON));
            } catch (final IOException ex) {
                response.setStatusCode(500);
            }
        };
    }

    private static void awaitForCondition(Supplier<Boolean> condition) throws InterruptedException {
        while (!condition.get()) {
            Thread.sleep(10);
        }
    }

    private void awaitForFlushToFinish() throws InterruptedException {
        lock.lock();
        try {
            flushed.await();
        } finally {
            lock.unlock();
        }
    }
}

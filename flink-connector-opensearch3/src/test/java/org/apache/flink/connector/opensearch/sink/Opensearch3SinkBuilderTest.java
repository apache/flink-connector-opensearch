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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Opensearch3SinkBuilder}. */
@ExtendWith(TestLoggerExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Opensearch3SinkBuilderTest {

    @TestFactory
    Stream<DynamicTest> testValidBuilders() {
        Stream<Opensearch3SinkBuilder<Object>> validBuilders =
                Stream.of(
                        createMinimalBuilder(),
                        createMinimalBuilder()
                                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE),
                        createMinimalBuilder()
                                .setBulkFlushBackoffStrategy(FlushBackoffType.CONSTANT, 1, 1),
                        createMinimalBuilder()
                                .setConnectionUsername("username")
                                .setConnectionPassword("password"),
                        createMinimalBuilder()
                                .setHttpClientConfigCallback(new NoOpHttpClientConfigCallback()));

        return DynamicTest.stream(
                validBuilders,
                Opensearch3SinkBuilder::toString,
                builder -> assertThatNoException().isThrownBy(builder::build));
    }

    @Test
    void testThrowIfExactlyOnceConfigured() {
        assertThatThrownBy(
                        () ->
                                createMinimalBuilder()
                                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testThrowIfHostsNotSet() {
        assertThatThrownBy(
                        () ->
                                createEmptyBuilder()
                                        .setEmitter((element, context, indexer) -> {})
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowIfEmitterNotSet() {
        assertThatThrownBy(
                        () ->
                                createEmptyBuilder()
                                        .setHosts(new HttpHost("http", "localhost", 3000))
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowIfHttpClientConfigCallbackNotSerializable() {
        assertThatThrownBy(
                        () ->
                                createMinimalBuilder()
                                        .setHttpClientConfigCallback(
                                                new NonSerializableHttpClientConfigCallback()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serializable");
    }

    @Test
    void testThrowIfHttpClientConfigCallbackNull() {
        assertThatThrownBy(
                        () -> createMinimalBuilder().setHttpClientConfigCallback(null).build())
                .isInstanceOf(NullPointerException.class);
    }

    private Opensearch3SinkBuilder<Object> createEmptyBuilder() {
        return new Opensearch3SinkBuilder<>();
    }

    private Opensearch3SinkBuilder<Object> createMinimalBuilder() {
        return new Opensearch3SinkBuilder<>()
                .setEmitter((element, context, indexer) -> {})
                .setHosts(new HttpHost("http", "localhost", 3000));
    }

    /** Serializable no-op callback for tests. */
    private static final class NoOpHttpClientConfigCallback
            implements Opensearch3HttpClientConfigCallback {

        private static final long serialVersionUID = 1L;

        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder;
        }
    }

    /** Holds a non-serializable field so {@link org.apache.flink.util.InstantiationUtil} rejects it. */
    private static final class NonSerializableHttpClientConfigCallback
            implements Opensearch3HttpClientConfigCallback {

        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unused")
        private final Thread nonSerializable = Thread.currentThread();

        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder;
        }
    }
}

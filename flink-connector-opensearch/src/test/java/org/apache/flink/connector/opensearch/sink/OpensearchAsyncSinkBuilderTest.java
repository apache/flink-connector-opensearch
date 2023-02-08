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

import org.apache.flink.util.TestLoggerExtension;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OpensearchAsyncSinkBuilder}. */
@ExtendWith(TestLoggerExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpensearchAsyncSinkBuilderTest {

    @TestFactory
    Stream<DynamicTest> testValidBuilders() {
        Stream<OpensearchAsyncSinkBuilder<Object>> validBuilders =
                Stream.of(
                        createMinimalBuilder(),
                        createMinimalBuilder()
                                .setConnectionUsername("username")
                                .setConnectionPassword("password"));

        return DynamicTest.stream(
                validBuilders,
                OpensearchAsyncSinkBuilder::toString,
                builder -> assertThatNoException().isThrownBy(builder::build));
    }

    @Test
    void testThrowIfHostsNotSet() {
        assertThatThrownBy(
                        () ->
                                createEmptyBuilder()
                                        .setElementConverter((element, context) -> null)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowIfElementConverterNotSet() {
        assertThatThrownBy(
                        () -> createEmptyBuilder().setHosts(new HttpHost("localhost:3000")).build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowIfSetInvalidTimeouts() {
        assertThatThrownBy(() -> createEmptyBuilder().setConnectionRequestTimeout(-1).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> createEmptyBuilder().setConnectionTimeout(-1).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> createEmptyBuilder().setSocketTimeout(-1).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    private OpensearchAsyncSinkBuilder<Object> createEmptyBuilder() {
        return new OpensearchAsyncSinkBuilder<>();
    }

    private OpensearchAsyncSinkBuilder<Object> createMinimalBuilder() {
        return new OpensearchAsyncSinkBuilder<>()
                .setElementConverter((element, context) -> null)
                .setHosts(new HttpHost("localhost:3000"));
    }
}

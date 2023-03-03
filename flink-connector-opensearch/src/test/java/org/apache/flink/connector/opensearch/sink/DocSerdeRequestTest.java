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

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.index.VersionType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocSerdeRequestTest {
    @ParameterizedTest
    @MethodSource("requests")
    void serde(DocWriteRequest<?> request) throws IOException {
        final DocSerdeRequest serialized = DocSerdeRequest.from(request);

        try (final ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            try (final DataOutputStream out = new DataOutputStream(bytes)) {
                serialized.writeTo(out);
            }

            try (final DataInputStream in =
                    new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                final DocSerdeRequest deserialized = DocSerdeRequest.readFrom(Byte.MAX_VALUE, in);
                assertThat(deserialized.getRequest())
                        .usingRecursiveComparison(
                                RecursiveComparisonConfiguration.builder()
                                        /* ignoring 'type', it is deprecated but backfilled for 1.x compatibility */
                                        .withIgnoredFields("type", "doc.type")
                                        .build())
                        .isEqualTo(serialized.getRequest());
            }
        }
    }

    @Test
    void unsupportedRequestType() throws IOException {
        final DocSerdeRequest serialized = DocSerdeRequest.from(new DummyDocWriteRequest());
        try (final ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            try (final DataOutputStream out = new DataOutputStream(bytes)) {
                assertThatThrownBy(() -> serialized.writeTo(out))
                        .isInstanceOf(IllegalStateException.class);
            }
        }
    }

    private static Stream<Arguments> requests() {
        return Stream.of(
                Arguments.of(new DeleteRequest("index").id("id")),
                Arguments.of(
                        new UpdateRequest()
                                .index("index")
                                .id("id")
                                .doc(Collections.singletonMap("action", "update"))),
                Arguments.of(
                        new IndexRequest("index")
                                .id("id")
                                .source(Collections.singletonMap("action", "index"))));
    }

    private static class DummyDocWriteRequest implements DocWriteRequest<Object> {
        @Override
        public String[] indices() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ramBytesUsed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object index(String index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String index() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object type(String type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object defaultTypeIfNull(String defaultType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String id() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndicesOptions indicesOptions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object routing(String routing) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String routing() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long version() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object version(long version) {
            throw new UnsupportedOperationException();
        }

        @Override
        public VersionType versionType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object versionType(VersionType versionType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object setIfSeqNo(long seqNo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object setIfPrimaryTerm(long term) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ifSeqNo() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ifPrimaryTerm() {
            throw new UnsupportedOperationException();
        }

        @Override
        public OpType opType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRequireAlias() {
            throw new UnsupportedOperationException();
        }
    }
}

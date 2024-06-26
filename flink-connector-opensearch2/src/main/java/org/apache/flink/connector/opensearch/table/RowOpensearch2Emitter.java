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

package org.apache.flink.connector.opensearch.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.opensearch.sink.OpensearchEmitter;
import org.apache.flink.connector.opensearch.sink.RequestIndexer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;

import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Sink function for converting upserts into Opensearch {@link ActionRequest}s. */
class RowOpensearch2Emitter implements OpensearchEmitter<RowData> {

    private final IndexGenerator indexGenerator;
    private final SerializationSchema<RowData> serializationSchema;
    private final XContentType contentType;
    private final Function<RowData, String> createKey;

    public RowOpensearch2Emitter(
            IndexGenerator indexGenerator,
            SerializationSchema<RowData> serializationSchema,
            XContentType contentType,
            Function<RowData, String> createKey) {
        this.indexGenerator = checkNotNull(indexGenerator);
        this.serializationSchema = checkNotNull(serializationSchema);
        this.contentType = checkNotNull(contentType);
        this.createKey = checkNotNull(createKey);
    }

    @Override
    public void open() throws Exception {
        try {
            serializationSchema.open(
                    new SerializationSchema.InitializationContext() {
                        @Override
                        public MetricGroup getMetricGroup() {
                            return new UnregisteredMetricsGroup();
                        }

                        @Override
                        public UserCodeClassLoader getUserCodeClassLoader() {
                            return SimpleUserCodeClassLoader.create(
                                    RowOpensearch2Emitter.class.getClassLoader());
                        }
                    });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to initialize serialization schema.", e);
        }
        indexGenerator.open();
    }

    @Override
    public void emit(RowData element, SinkWriter.Context context, RequestIndexer indexer) {
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                processUpsert(element, indexer);
                break;
            case UPDATE_BEFORE:
            case DELETE:
                processDelete(element, indexer);
                break;
            default:
                throw new TableException("Unsupported message kind: " + element.getRowKind());
        }
    }

    private void processUpsert(RowData row, RequestIndexer indexer) {
        final byte[] document = serializationSchema.serialize(row);
        final String key = createKey.apply(row);
        if (key != null) {
            final UpdateRequest updateRequest =
                    new UpdateRequest(indexGenerator.generate(row), key)
                            .doc(document, contentType)
                            .upsert(document, contentType);
            indexer.add(updateRequest);
        } else {
            final IndexRequest indexRequest =
                    new IndexRequest(indexGenerator.generate(row))
                            .id(key)
                            .source(document, contentType);
            indexer.add(indexRequest);
        }
    }

    private void processDelete(RowData row, RequestIndexer indexer) {
        final String key = createKey.apply(row);
        final DeleteRequest deleteRequest = new DeleteRequest(indexGenerator.generate(row), key);
        indexer.add(deleteRequest);
    }
}

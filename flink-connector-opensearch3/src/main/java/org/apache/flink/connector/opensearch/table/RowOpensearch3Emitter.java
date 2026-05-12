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
import org.apache.flink.connector.opensearch.sink.Opensearch3Emitter;
import org.apache.flink.connector.opensearch.sink.Opensearch3RequestIndexer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Sink emitter for converting upserts into OpenSearch 3.x bulk operations. */
class RowOpensearch3Emitter implements Opensearch3Emitter<RowData> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final IndexGenerator indexGenerator;
    private final SerializationSchema<RowData> serializationSchema;
    private final Function<RowData, String> createKey;

    public RowOpensearch3Emitter(
            IndexGenerator indexGenerator,
            SerializationSchema<RowData> serializationSchema,
            Function<RowData, String> createKey) {
        this.indexGenerator = checkNotNull(indexGenerator);
        this.serializationSchema = checkNotNull(serializationSchema);
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
                                    RowOpensearch3Emitter.class.getClassLoader());
                        }
                    });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to initialize serialization schema.", e);
        }
        indexGenerator.open();
    }

    @Override
    public void emit(
            RowData element, SinkWriter.Context context, Opensearch3RequestIndexer indexer) {
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

    private void processUpsert(RowData row, Opensearch3RequestIndexer indexer) {
        final byte[] document = serializationSchema.serialize(row);
        final String key = createKey.apply(row);
        final String index = indexGenerator.generate(row);
        final Map<String, Object> docMap = parseDocument(document);

        if (key != null) {
            // Use upsert for rows with keys
            indexer.addUpsertRequest(index, key, docMap, docMap);
        } else {
            // Use index for rows without keys (auto-generated id)
            indexer.addIndexRequest(index, null, docMap);
        }
    }

    private void processDelete(RowData row, Opensearch3RequestIndexer indexer) {
        final String key = createKey.apply(row);
        final String index = indexGenerator.generate(row);
        indexer.addDeleteRequest(index, key);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseDocument(byte[] document) {
        try {
            return OBJECT_MAPPER.readValue(document, HashMap.class);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to parse document as JSON", e);
        }
    }
}

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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * Creates none or multiple bulk operations from the incoming elements for OpenSearch 3.x.
 *
 * <p>This is used by sinks to prepare elements for sending them to OpenSearch.
 *
 * <p>Example:
 *
 * <pre>{@code
 * private static class TestOpensearch3Emitter implements Opensearch3Emitter<Tuple2<Integer, String>> {
 *
 *     public void emit(Tuple2<Integer, String> element, SinkWriter.Context context, Opensearch3RequestIndexer indexer) {
 *         Map<String, Object> document = new HashMap<>();
 *         document.put("data", element.f1);
 *
 *         indexer.addIndexRequest(
 *             "my-index",
 *             element.f0.toString(),
 *             document
 *         );
 *     }
 * }
 * }</pre>
 *
 * @param <T> The type of the element handled by this {@link Opensearch3Emitter}
 */
@PublicEvolving
public interface Opensearch3Emitter<T> extends Function {

    /**
     * Initialization method for the function. It is called once before the actual working process
     * methods.
     */
    default void open() throws Exception {}

    /** Tear-down method for the function. It is called when the sink closes. */
    default void close() throws Exception {}

    /**
     * Process the incoming element to produce multiple bulk operations. The produced operations
     * should be added to the provided {@link Opensearch3RequestIndexer}.
     *
     * @param element incoming element to process
     * @param context to access additional information about the record
     * @param indexer request indexer that operations should be added to
     */
    void emit(T element, SinkWriter.Context context, Opensearch3RequestIndexer indexer);
}

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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.opensearch.sink.OpensearchEmitter;
import org.apache.flink.connector.opensearch.sink.RequestIndexer;

import org.opensearch.action.update.UpdateRequest;

import java.util.HashMap;
import java.util.Map;

/** Test emitter for performing Opensearch indexing requests. */
public class OpensearchTestEmitter implements OpensearchEmitter<Tuple2<Integer, String>> {
    private static final long serialVersionUID = 1L;
    private final String indexName;

    OpensearchTestEmitter(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public void emit(
            Tuple2<Integer, String> element, SinkWriter.Context context, RequestIndexer indexer) {
        final Map<String, Object> json = new HashMap<>();
        json.put("key", element.f0);
        json.put("value", element.f1);

        final UpdateRequest updateRequest =
                new UpdateRequest(indexName, String.valueOf(element.f0)).doc(json).upsert(json);
        indexer.add(updateRequest);
    }
}

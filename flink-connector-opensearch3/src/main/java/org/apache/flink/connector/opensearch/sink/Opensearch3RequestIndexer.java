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

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Users add multiple delete, index or update requests to an {@link Opensearch3RequestIndexer} to
 * prepare them for sending to an OpenSearch 3.x cluster.
 *
 * <p>This interface is specific to OpenSearch 3.x and uses the new opensearch-java client API
 * patterns.
 */
@PublicEvolving
public interface Opensearch3RequestIndexer {

    /**
     * Add an index request to the indexer.
     *
     * @param index the target index name
     * @param id the document id (can be null for auto-generated ids)
     * @param document the document to index as a Map
     */
    void addIndexRequest(String index, @Nullable String id, Map<String, Object> document);

    /**
     * Add a delete request to the indexer.
     *
     * @param index the target index name
     * @param id the document id to delete
     */
    void addDeleteRequest(String index, String id);

    /**
     * Add an update request to the indexer.
     *
     * @param index the target index name
     * @param id the document id to update
     * @param document the partial document for update
     */
    void addUpdateRequest(String index, String id, Map<String, Object> document);

    /**
     * Add an upsert request to the indexer.
     *
     * @param index the target index name
     * @param id the document id to upsert
     * @param document the document for update
     * @param upsertDocument the document to use if the document doesn't exist
     */
    void addUpsertRequest(
            String index,
            String id,
            Map<String, Object> document,
            Map<String, Object> upsertDocument);
}

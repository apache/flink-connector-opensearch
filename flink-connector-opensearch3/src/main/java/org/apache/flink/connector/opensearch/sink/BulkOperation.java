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

import org.apache.flink.annotation.Internal;

import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializable wrapper for OpenSearch 3 bulk operations. Since the opensearch-java client's
 * BulkOperation is not serializable, we create our own representation that can be serialized and
 * later converted to the actual BulkOperation.
 */
@Internal
public class BulkOperation implements Serializable {

    private static final long serialVersionUID = 1L;

    private final OperationType operationType;
    private final String index;
    @Nullable private final String id;
    @Nullable private final Map<String, Object> document;
    @Nullable private final Map<String, Object> upsertDocument;

    /** The type of bulk operation. */
    public enum OperationType {
        INDEX,
        DELETE,
        UPDATE
    }

    private BulkOperation(
            OperationType operationType,
            String index,
            @Nullable String id,
            @Nullable Map<String, Object> document,
            @Nullable Map<String, Object> upsertDocument) {
        this.operationType = checkNotNull(operationType);
        this.index = checkNotNull(index);
        this.id = id;
        this.document = document;
        this.upsertDocument = upsertDocument;
    }

    /**
     * Creates an index operation.
     *
     * @param index the target index
     * @param id the document id (can be null for auto-generated ids)
     * @param document the document to index
     * @return the bulk operation
     */
    public static BulkOperation index(
            String index, @Nullable String id, Map<String, Object> document) {
        checkNotNull(document, "Document cannot be null for index operations");
        return new BulkOperation(OperationType.INDEX, index, id, document, null);
    }

    /**
     * Creates a delete operation.
     *
     * @param index the target index
     * @param id the document id to delete
     * @return the bulk operation
     */
    public static BulkOperation delete(String index, String id) {
        checkNotNull(id, "Document ID cannot be null for delete operations");
        return new BulkOperation(OperationType.DELETE, index, id, null, null);
    }

    /**
     * Creates an update operation.
     *
     * @param index the target index
     * @param id the document id to update
     * @param document the partial document for update
     * @param upsertDocument the document to use if the document doesn't exist (can be null)
     * @return the bulk operation
     */
    public static BulkOperation update(
            String index,
            String id,
            Map<String, Object> document,
            @Nullable Map<String, Object> upsertDocument) {
        checkNotNull(id, "Document ID cannot be null for update operations");
        checkNotNull(document, "Document cannot be null for update operations");
        return new BulkOperation(OperationType.UPDATE, index, id, document, upsertDocument);
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public String getIndex() {
        return index;
    }

    @Nullable
    public String getId() {
        return id;
    }

    @Nullable
    public Map<String, Object> getDocument() {
        return document;
    }

    @Nullable
    public Map<String, Object> getUpsertDocument() {
        return upsertDocument;
    }

    /**
     * Converts this serializable operation to the opensearch-java client's BulkOperation.
     *
     * @return the client BulkOperation
     */
    public org.opensearch.client.opensearch.core.bulk.BulkOperation toClientBulkOperation() {
        switch (operationType) {
            case INDEX:
                IndexOperation.Builder<Map<String, Object>> indexBuilder =
                        new IndexOperation.Builder<Map<String, Object>>()
                                .index(index)
                                .document(document);
                if (id != null) {
                    indexBuilder.id(id);
                }
                return new org.opensearch.client.opensearch.core.bulk.BulkOperation.Builder()
                        .index(indexBuilder.build())
                        .build();

            case DELETE:
                DeleteOperation.Builder deleteBuilder =
                        new DeleteOperation.Builder().index(index).id(id);
                return new org.opensearch.client.opensearch.core.bulk.BulkOperation.Builder()
                        .delete(deleteBuilder.build())
                        .build();

            case UPDATE:
                // For updates, we use index operation with the same ID (upsert behavior)
                // This achieves the same effect as an update with upsert in OpenSearch
                IndexOperation.Builder<Map<String, Object>> updateAsIndexBuilder =
                        new IndexOperation.Builder<Map<String, Object>>()
                                .index(index)
                                .id(id)
                                .document(upsertDocument != null ? upsertDocument : document);
                return new org.opensearch.client.opensearch.core.bulk.BulkOperation.Builder()
                        .index(updateAsIndexBuilder.build())
                        .build();

            default:
                throw new IllegalStateException("Unknown operation type: " + operationType);
        }
    }

    /**
     * Estimates the size of this operation in bytes.
     *
     * @return estimated size in bytes
     */
    public long estimateSizeInBytes() {
        // Rough estimation: index + id + document serialization overhead
        long size = index.length() * 2L; // UTF-16 chars
        if (id != null) {
            size += id.length() * 2L;
        }
        if (document != null) {
            // Rough estimate: assume average of 50 bytes per entry
            size += document.size() * 50L;
        }
        if (upsertDocument != null) {
            size += upsertDocument.size() * 50L;
        }
        return size;
    }

    @Override
    public String toString() {
        return "BulkOperation{"
                + "operationType="
                + operationType
                + ", index='"
                + index
                + '\''
                + ", id='"
                + id
                + '\''
                + '}';
    }
}

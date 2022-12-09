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

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.http.HttpHost;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The type Opensearch test client. */
public class OpensearchTestClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OpensearchTestClient.class);

    private final RestHighLevelClient restClient;

    /**
     * Instantiates a new Opensearch client.
     *
     * @param address The address to access Opensearch from the host machine (outside of the
     *     containerized environment).
     */
    public OpensearchTestClient(String address) {
        checkNotNull(address);
        this.restClient = new RestHighLevelClient(RestClient.builder(HttpHost.create(address)));
    }

    public void deleteIndex(String indexName) {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        try {
            restClient.indices().delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Cannot delete index {}", indexName, e);
        }
        // This is needed to avoid race conditions between tests that reuse the same index
        refreshIndex(indexName);
    }

    public void refreshIndex(String indexName) {
        RefreshRequest refresh = new RefreshRequest(indexName);
        try {
            restClient.indices().refresh(refresh, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Cannot delete index {}", indexName, e);
        } catch (OpenSearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                LOG.info("Index {} not found", indexName);
            }
        }
    }

    public void createIndexIfDoesNotExist(String indexName, int shards, int replicas) {
        GetIndexRequest request = new GetIndexRequest(indexName);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(
                Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", replicas));
        try {
            boolean exists = restClient.indices().exists(request, RequestOptions.DEFAULT);
            if (!exists) {
                restClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            } else {
                LOG.info("Index already exists {}", indexName);
            }
        } catch (IOException e) {
            LOG.error("Cannot create index {}", indexName, e);
        }
    }

    @Override
    public void close() throws Exception {
        restClient.close();
    }

    public List<Tuple2<Integer, String>> fetchAll(
            String indexName, String sortField, int from, int pageLength, boolean trackTotalHits) {
        try {
            SearchResponse response =
                    restClient.search(
                            new SearchRequest(indexName)
                                    .source(
                                            new SearchSourceBuilder()
                                                    .sort(sortField, SortOrder.ASC)
                                                    .from(from)
                                                    .size(pageLength)
                                                    .trackTotalHits(trackTotalHits)),
                            RequestOptions.DEFAULT);
            SearchHit[] searchHits = response.getHits().getHits();
            return Arrays.stream(searchHits)
                    .map(
                            searchHit ->
                                    ComparableTuple2.of(
                                            Integer.valueOf(searchHit.getId()),
                                            searchHit.getSourceAsMap().get("value").toString()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOG.error("Fetching records failed", e);
            return Collections.emptyList();
        }
    }
}

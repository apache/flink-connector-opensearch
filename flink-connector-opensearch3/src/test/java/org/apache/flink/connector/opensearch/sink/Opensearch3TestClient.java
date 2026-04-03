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

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.testcontainers.OpensearchContainer;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test client for OpenSearch 3.x using the opensearch-java client. */
public class Opensearch3TestClient implements Closeable {

    private static final String DATA_FIELD_NAME = "data";

    /**
     * Poll interval while waiting for bulk-indexed documents to become visible (matches
     * Opensearch2TestClient).
     */
    private static final long INDEX_VISIBILITY_POLL_INTERVAL_MS = 10L;

    private static final long INDEX_VISIBILITY_TIMEOUT_MS = 60_000L;

    private final OpenSearchClient client;
    private final OpenSearchTransport transport;

    public Opensearch3TestClient(OpensearchContainer container) throws URISyntaxException {
        HttpHost host = HttpHost.create(container.getHttpHostAddress());

        ApacheHttpClient5TransportBuilder builder = ApacheHttpClient5TransportBuilder.builder(host);

        builder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(
                            new AuthScope(null, -1),
                            new UsernamePasswordCredentials(
                                    container.getUsername(),
                                    container.getPassword().toCharArray()));
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

                    try {
                        TlsStrategy tlsStrategy =
                                ClientTlsStrategyBuilder.create()
                                        .setSslContext(
                                                SSLContextBuilder.create()
                                                        .loadTrustMaterial(
                                                                null, (chain, authType) -> true)
                                                        .build())
                                        .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                        .build();
                        PoolingAsyncClientConnectionManager connectionManager =
                                PoolingAsyncClientConnectionManagerBuilder.create()
                                        .setTlsStrategy(tlsStrategy)
                                        .build();
                        httpClientBuilder.setConnectionManager(connectionManager);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to configure SSL", e);
                    }

                    return httpClientBuilder;
                });

        this.transport = builder.build();
        this.client = new OpenSearchClient(transport);
    }

    public String getDataFieldName() {
        return DATA_FIELD_NAME;
    }

    public static String buildMessage(int id) {
        return "message #" + id;
    }

    public void assertThatIdsAreWritten(String index, Integer... ids) throws Exception {
        final long deadline = System.currentTimeMillis() + INDEX_VISIBILITY_TIMEOUT_MS;
        List<String> retrievedIds = List.of();

        while (true) {
            client.indices().refresh(r -> r.index(index));
            SearchRequest searchRequest =
                    new SearchRequest.Builder().index(index).size(100).build();
            SearchResponse<Map> response = client.search(searchRequest, Map.class);
            retrievedIds =
                    response.hits().hits().stream().map(Hit::id).collect(Collectors.toList());

            boolean allPresent = true;
            for (Integer id : ids) {
                if (!retrievedIds.contains(id.toString())) {
                    allPresent = false;
                    break;
                }
            }
            if (allPresent) {
                break;
            }
            if (System.currentTimeMillis() >= deadline) {
                assertThat(retrievedIds)
                        .as(
                                "Timeout waiting for documents in index=%s; expected ids=%s",
                                index, Arrays.toString(ids))
                        .containsAll(
                                Arrays.stream(ids)
                                        .map(String::valueOf)
                                        .collect(Collectors.toList()));
            }
            Thread.sleep(INDEX_VISIBILITY_POLL_INTERVAL_MS);
        }

        for (Integer id : ids) {
            assertThat(retrievedIds).contains(id.toString());
        }
    }

    @Override
    public void close() throws IOException {
        transport.close();
    }
}

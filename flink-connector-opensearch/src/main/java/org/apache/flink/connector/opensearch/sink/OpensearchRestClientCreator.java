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

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/** The utility class to encapsulate {@link RestHighLevelClient} creation. */
class OpensearchRestClientCreator {
    /** Utility class. */
    private OpensearchRestClientCreator() {}

    /**
     * Creates new instance of {@link RestHighLevelClient}.
     *
     * @param hosts list of hosts to connect
     * @param networkClientConfig client network configuration
     * @return new instance of {@link RestHighLevelClient}
     */
    static RestHighLevelClient create(
            final List<HttpHost> hosts, final NetworkClientConfig networkClientConfig) {
        return new RestHighLevelClient(
                configureRestClientBuilder(
                        RestClient.builder(hosts.toArray(new HttpHost[0])), networkClientConfig));
    }

    private static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, NetworkClientConfig networkClientConfig) {
        if (networkClientConfig.getConnectionPathPrefix() != null) {
            builder.setPathPrefix(networkClientConfig.getConnectionPathPrefix());
        }

        builder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    if (networkClientConfig.getPassword() != null
                            && networkClientConfig.getUsername() != null) {
                        final CredentialsProvider credentialsProvider =
                                new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(
                                AuthScope.ANY,
                                new UsernamePasswordCredentials(
                                        networkClientConfig.getUsername(),
                                        networkClientConfig.getPassword()));

                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }

                    if (networkClientConfig.isAllowInsecure().orElse(false)) {
                        try {
                            httpClientBuilder.setSSLContext(
                                    SSLContexts.custom()
                                            .loadTrustMaterial(new TrustAllStrategy())
                                            .build());
                        } catch (final NoSuchAlgorithmException
                                | KeyStoreException
                                | KeyManagementException ex) {
                            throw new IllegalStateException(
                                    "Unable to create custom SSL context", ex);
                        }
                    }

                    return httpClientBuilder;
                });
        if (networkClientConfig.getConnectionRequestTimeout() != null
                || networkClientConfig.getConnectionTimeout() != null
                || networkClientConfig.getSocketTimeout() != null) {
            builder.setRequestConfigCallback(
                    requestConfigBuilder -> {
                        if (networkClientConfig.getConnectionRequestTimeout() != null) {
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    networkClientConfig.getConnectionRequestTimeout());
                        }
                        if (networkClientConfig.getConnectionTimeout() != null) {
                            requestConfigBuilder.setConnectTimeout(
                                    networkClientConfig.getConnectionTimeout());
                        }
                        if (networkClientConfig.getSocketTimeout() != null) {
                            requestConfigBuilder.setSocketTimeout(
                                    networkClientConfig.getSocketTimeout());
                        }
                        return requestConfigBuilder;
                    });
        }
        return builder;
    }
}

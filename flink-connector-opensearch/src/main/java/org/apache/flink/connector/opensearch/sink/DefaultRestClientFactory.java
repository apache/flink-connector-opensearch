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

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.client.RestClientBuilder;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

/** Provides the default implementation for {@link RestClientFactory}. */
public class DefaultRestClientFactory implements RestClientFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public void configureRestClientBuilder(
            RestClientBuilder builder, RestClientConfig networkClientConfig) {

        if (networkClientConfig.getConnectionPathPrefix() != null) {
            builder.setPathPrefix(networkClientConfig.getConnectionPathPrefix());
        }

        builder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    configureHttpClientBuilder(httpClientBuilder, networkClientConfig);
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
    }

    protected void configureHttpClientBuilder(
            HttpAsyncClientBuilder httpClientBuilder, RestClientConfig networkClientConfig) {
        if (networkClientConfig.getPassword() != null
                && networkClientConfig.getUsername() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(
                            networkClientConfig.getUsername(), networkClientConfig.getPassword()));

            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }

        if (networkClientConfig.isAllowInsecure().orElse(false)) {
            try {
                httpClientBuilder.setSSLContext(
                        SSLContexts.custom().loadTrustMaterial(new TrustAllStrategy()).build());
            } catch (final NoSuchAlgorithmException
                    | KeyStoreException
                    | KeyManagementException ex) {
                throw new IllegalStateException("Unable to create custom SSL context", ex);
            }
        }
    }
}

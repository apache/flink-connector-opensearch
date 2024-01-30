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

import org.apache.flink.annotation.PublicEvolving;

import org.opensearch.client.RestClientBuilder;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * A factory that is used to configure the {@link org.opensearch.client.RestHighLevelClient}
 * internally used in the {@link OpensearchSink}.
 */
@PublicEvolving
public interface RestClientFactory extends Serializable {

    /** The REST client configuration. */
    @PublicEvolving
    interface RestClientConfig {
        /**
         * Gets the configured username.
         *
         * @return the configured username
         */
        @Nullable
        String getUsername();

        /**
         * Gets the configured password.
         *
         * @return the configured password
         */
        @Nullable
        String getPassword();

        /**
         * Gets the configured connection request timeout.
         *
         * @return the configured connection request timeout
         */
        @Nullable
        Integer getConnectionRequestTimeout();

        /**
         * Gets the configured connection timeout.
         *
         * @return the configured connection timeout
         */
        @Nullable
        Integer getConnectionTimeout();

        /**
         * Gets the configured socket timeout.
         *
         * @return the configured socket timeout
         */
        @Nullable
        Integer getSocketTimeout();

        /**
         * Gets the configured connection path prefix.
         *
         * @return the configured connection path prefix
         */
        @Nullable
        String getConnectionPathPrefix();

        /**
         * Returns if the insecure HTTPS connections are allowed or not (self-signed certificates,
         * etc).
         *
         * @return if the insecure HTTPS connections are allowed or not
         */
        Optional<Boolean> isAllowInsecure();
    }

    /**
     * Configures the rest client builder.
     *
     * @param restClientBuilder the configured REST client builder.
     * @param clientConfig the client network configuration
     */
    void configureRestClientBuilder(
            RestClientBuilder restClientBuilder, RestClientConfig clientConfig);
}

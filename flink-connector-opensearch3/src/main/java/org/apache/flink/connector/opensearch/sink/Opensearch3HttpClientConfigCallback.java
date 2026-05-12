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

import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.io.Serializable;

/**
 * Callback to customize the Apache HttpClient 5 async client used by the OpenSearch Java client
 * transport (for example to register AWS SigV4 request signing).
 *
 * <p>Implementations must be {@link Serializable} so they can be shipped with the Flink job. The
 * callback is invoked <strong>after</strong> the sink applies {@link
 * Opensearch3SinkBuilder#setConnectionUsername(String) username/password} and {@link
 * Opensearch3SinkBuilder#setAllowInsecure(boolean) allowInsecure} settings from the builder.
 *
 * @see Opensearch3SinkBuilder#setHttpClientConfigCallback(Opensearch3HttpClientConfigCallback)
 */
@PublicEvolving
@FunctionalInterface
public interface Opensearch3HttpClientConfigCallback
        extends ApacheHttpClient5TransportBuilder.HttpClientConfigCallback, Serializable {}

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

package org.apache.flink.streaming.tests;

import org.apache.flink.connector.testframe.external.ExternalContextFactory;

import org.opensearch.testcontainers.OpensearchContainer;

import java.net.URL;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Opensearch sink external context factory. */
class OpensearchSinkExternalContextFactory
        implements ExternalContextFactory<OpensearchSinkExternalContext> {
    /** The OpensearchContainer container. */
    private final OpensearchContainer opensearchContainer;

    /** The connector jars. */
    private final List<URL> connectorJars;

    /**
     * Instantiates a new Opensearch sink external context factory.
     *
     * @param opensearchContainer The Opensearch container.
     * @param connectorJars The connector jars.
     */
    OpensearchSinkExternalContextFactory(
            OpensearchContainer opensearchContainer, List<URL> connectorJars) {
        this.opensearchContainer = checkNotNull(opensearchContainer);
        this.connectorJars = checkNotNull(connectorJars);
    }

    @Override
    public OpensearchSinkExternalContext createExternalContext(String testName) {
        return new OpensearchSinkExternalContext(
                opensearchContainer.getHttpHostAddress(),
                opensearchContainer.getNetworkAliases().get(0)
                        + ":"
                        + opensearchContainer.getExposedPorts().get(0),
                connectorJars);
    }
}

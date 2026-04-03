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

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Configuration for bulk processing in OpenSearch 3.x sink. */
class BulkProcessorConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int bulkFlushMaxActions;
    /** -1 if bulk flush max size is disabled; otherwise max buffered bulk payload in bytes. */
    private final long bulkFlushMaxBytes;

    private final long bulkFlushInterval;
    private final FlushBackoffType flushBackoffType;
    private final int bulkFlushBackoffRetries;
    private final long bulkFlushBackOffDelay;

    BulkProcessorConfig(
            int bulkFlushMaxActions,
            long bulkFlushMaxBytes,
            long bulkFlushInterval,
            FlushBackoffType flushBackoffType,
            int bulkFlushBackoffRetries,
            long bulkFlushBackOffDelay) {
        this.bulkFlushMaxActions = bulkFlushMaxActions;
        this.bulkFlushMaxBytes = bulkFlushMaxBytes;
        this.bulkFlushInterval = bulkFlushInterval;
        this.flushBackoffType = checkNotNull(flushBackoffType);
        this.bulkFlushBackoffRetries = bulkFlushBackoffRetries;
        this.bulkFlushBackOffDelay = bulkFlushBackOffDelay;
    }

    public int getBulkFlushMaxActions() {
        return bulkFlushMaxActions;
    }

    public long getBulkFlushMaxBytes() {
        return bulkFlushMaxBytes;
    }

    public long getBulkFlushInterval() {
        return bulkFlushInterval;
    }

    public FlushBackoffType getFlushBackoffType() {
        return flushBackoffType;
    }

    public int getBulkFlushBackoffRetries() {
        return bulkFlushBackoffRetries;
    }

    public long getBulkFlushBackOffDelay() {
        return bulkFlushBackOffDelay;
    }
}

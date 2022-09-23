/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.opensearch.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.opensearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.opensearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ActionRequestFailureHandler} that re-adds requests that failed due to temporary {@link
 * OpenSearchRejectedExecutionException}s (which means that Opensearch node queues are currently
 * full), and fails for all other failures.
 *
 * @deprecated This hase been deprecated and will be removed in the future.
 */
@Deprecated
@PublicEvolving
public class RetryRejectedExecutionFailureHandler implements ActionRequestFailureHandler {

    private static final long serialVersionUID = -7423562912824511906L;

    private static final Logger LOG =
            LoggerFactory.getLogger(RetryRejectedExecutionFailureHandler.class);

    @Override
    public void onFailure(
            ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer)
            throws Throwable {
        LOG.error("Failed Opensearch item request: {}", failure.getMessage(), failure);
        if (ExceptionUtils.findThrowable(failure, OpenSearchRejectedExecutionException.class)
                .isPresent()) {
            indexer.add(action);
        } else {
            // rethrow all other failures
            throw failure;
        }
    }
}

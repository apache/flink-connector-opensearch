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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FlinkRuntimeException;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.rest.RestStatus;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A strict implementation that fails if either the whole bulk request failed or any of its actions.
 */
class DefaultBulkResponseInspector implements BulkResponseInspector {

    @VisibleForTesting final FailureHandler failureHandler;

    DefaultBulkResponseInspector() {
        this(new DefaultFailureHandler());
    }

    DefaultBulkResponseInspector(FailureHandler failureHandler) {
        this.failureHandler = checkNotNull(failureHandler);
    }

    @Override
    public void inspect(BulkRequest request, BulkResponse response) {
        if (!response.hasFailures()) {
            return;
        }

        Throwable chainedFailures = null;
        for (int i = 0; i < response.getItems().length; i++) {
            final BulkItemResponse itemResponse = response.getItems()[i];
            if (!itemResponse.isFailed()) {
                continue;
            }
            final Throwable failure = itemResponse.getFailure().getCause();
            if (failure == null) {
                continue;
            }
            final RestStatus restStatus = itemResponse.getFailure().getStatus();
            final DocWriteRequest<?> actionRequest = request.requests().get(i);

            chainedFailures =
                    firstOrSuppressed(
                            wrapException(restStatus, failure, actionRequest), chainedFailures);
        }
        if (chainedFailures == null) {
            return;
        }
        failureHandler.onFailure(chainedFailures);
    }

    private static Throwable wrapException(
            RestStatus restStatus, Throwable rootFailure, DocWriteRequest<?> actionRequest) {
        if (restStatus == null) {
            return new FlinkRuntimeException(
                    String.format("Single action %s of bulk request failed.", actionRequest),
                    rootFailure);
        } else {
            return new FlinkRuntimeException(
                    String.format(
                            "Single action %s of bulk request failed with status %s.",
                            actionRequest, restStatus.getStatus()),
                    rootFailure);
        }
    }
}

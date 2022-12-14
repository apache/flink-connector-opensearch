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

package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.annotation.PublicEvolving;

import org.opensearch.action.ActionRequest;

import java.io.Serializable;

/**
 * An implementation of {@link ActionRequestFailureHandler} is provided by the user to define how
 * failed {@link ActionRequest ActionRequests} should be handled, e.g. dropping them, reprocessing
 * malformed documents, or simply requesting them to be sent to Opensearch again if the failure is
 * only temporary.
 *
 * <p>Example:
 *
 * <pre>{@code
 * private static class ExampleActionRequestFailureHandler implements ActionRequestFailureHandler {
 *
 * 	@Override
 * 	void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
 * 		if (ExceptionUtils.findThrowable(failure, OpenSearchRejectedExecutionException.class).isPresent()) {
 * 			// full queue; re-add document for indexing
 * 			indexer.add(action);
 * 		} else if (ExceptionUtils.findThrowable(failure, OpensearchParseException.class).isPresent()) {
 * 			// malformed document; simply drop request without failing sink
 * 		} else {
 * 			// for all other failures, fail the sink;
 * 			// here the failure is simply rethrown, but users can also choose to throw custom exceptions
 * 			throw failure;
 * 		}
 * 	}
 * }
 *
 * }</pre>
 *
 * <p>The above example will let the sink re-add requests that failed due to queue capacity
 * saturation and drop requests with malformed documents, without failing the sink. For all other
 * failures, the sink will fail.
 *
 * @deprecated This has been deprecated and will be removed in the future.
 */
@Deprecated
@PublicEvolving
public interface ActionRequestFailureHandler extends Serializable {

    /**
     * Handle a failed {@link ActionRequest}.
     *
     * @param action the {@link ActionRequest} that failed due to the failure
     * @param failure the cause of failure
     * @param restStatusCode the REST status code of the failure (-1 if none can be retrieved)
     * @param indexer request indexer to re-add the failed action, if intended to do so
     * @throws Throwable if the sink should fail on this failure, the implementation should rethrow
     *     the exception or a custom one
     */
    void onFailure(
            ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer)
            throws Throwable;
}

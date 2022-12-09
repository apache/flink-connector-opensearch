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

package org.apache.flink.streaming.tests;

import org.apache.flink.connector.opensearch.test.DockerImageVersions;
import org.apache.flink.connector.testframe.container.FlinkContainerTestEnvironment;
import org.apache.flink.connector.testframe.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SinkTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.test.resources.ResourceTestUtils;

import org.opensearch.testcontainers.OpensearchContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.testframe.utils.CollectIteratorAssertions.assertThat;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;

/** End to end test for OpensearchSink based on connector testing framework. */
@SuppressWarnings("unused")
public class OpensearchSinkE2ECase extends SinkTestSuiteBase<ComparableTuple2<Integer, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(OpensearchSinkE2ECase.class);
    private static final int READER_RETRY_ATTEMPTS = 10;
    private static final int READER_TIMEOUT = -1; // Not used

    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    @TestEnv FlinkContainerTestEnvironment flink = new FlinkContainerTestEnvironment(1, 6);

    public OpensearchSinkE2ECase() throws Exception {}

    @TestExternalSystem
    DefaultContainerizedExternalSystem<OpensearchContainer> opensearch =
            DefaultContainerizedExternalSystem.builder()
                    .fromContainer(
                            new OpensearchContainer(
                                            DockerImageName.parse(DockerImageVersions.OPENSEARCH_1))
                                    .withEnv(
                                            "cluster.routing.allocation.disk.threshold_enabled",
                                            "false")
                                    .withNetworkAliases("opensearch"))
                    .bindWithFlinkContainer(flink.getFlinkContainers().getJobManager())
                    .build();

    @TestContext
    OpensearchSinkExternalContextFactory contextFactory =
            new OpensearchSinkExternalContextFactory(
                    opensearch.getContainer(),
                    Arrays.asList(
                            ResourceTestUtils.getResource(
                                            "dependencies/opensearch-end-to-end-test.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL(),
                            ResourceTestUtils.getResource(
                                            "dependencies/flink-connector-test-utils.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL()));

    @Override
    protected void checkResultWithSemantic(
            ExternalSystemDataReader<ComparableTuple2<Integer, String>> reader,
            List<ComparableTuple2<Integer, String>> testData,
            CheckpointingMode semantic)
            throws Exception {
        waitUntilCondition(
                () -> {
                    try {
                        List<ComparableTuple2<Integer, String>> result =
                                reader.poll(Duration.ofMillis(READER_TIMEOUT));
                        assertThat(sort(result).iterator())
                                .matchesRecordsFromSource(
                                        Collections.singletonList(sort(testData)), semantic);
                        return true;
                    } catch (Throwable t) {
                        LOG.warn("Polled results not as expected", t);
                        return false;
                    }
                },
                5000,
                READER_RETRY_ATTEMPTS);
    }

    private static <T extends Comparable<T>> List<T> sort(List<T> list) {
        Collections.sort(list);
        return list;
    }
}

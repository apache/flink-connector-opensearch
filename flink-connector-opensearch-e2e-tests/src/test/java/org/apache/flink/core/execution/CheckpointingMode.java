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

package org.apache.flink.core.execution;

import org.apache.flink.streaming.tests.OpensearchSinkE2ECase;

/**
 * This is a copy of {@link CheckpointingMode} from flink-core module introduced in Flink 1.20. We
 * need it here to make {@link OpensearchSinkE2ECase} compatible with earlier releases. Could be
 * removed together with dropping support of Flink 1.19.
 */
public enum CheckpointingMode {
    EXACTLY_ONCE,
    AT_LEAST_ONCE;

    private CheckpointingMode() {}
}

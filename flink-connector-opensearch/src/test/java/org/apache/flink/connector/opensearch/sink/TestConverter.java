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

import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

class TestConverter implements ElementConverter<Tuple2<Integer, String>, DocWriteRequest<?>> {

    private final String index;
    private final XContentBuilderProvider xContentBuilderProvider;
    private final String dataFieldName;

    public static TestConverter jsonConverter(String index, String dataFieldName) {
        return new TestConverter(index, dataFieldName, XContentFactory::jsonBuilder);
    }

    public static TestConverter smileConverter(String index, String dataFieldName) {
        return new TestConverter(index, dataFieldName, XContentFactory::smileBuilder);
    }

    private TestConverter(
            String index, String dataFieldName, XContentBuilderProvider xContentBuilderProvider) {
        this.dataFieldName = dataFieldName;
        this.index = index;
        this.xContentBuilderProvider = xContentBuilderProvider;
    }

    @Override
    public DocWriteRequest<?> apply(Tuple2<Integer, String> element, Context context) {
        return createIndexRequest(element);
    }

    public IndexRequest createIndexRequest(Tuple2<Integer, String> element) {
        Map<String, Object> document = new HashMap<>();
        document.put(dataFieldName, element.f1);
        try {
            return new IndexRequest(index)
                    .id(element.f0.toString())
                    .source(xContentBuilderProvider.getBuilder().map(document));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface XContentBuilderProvider extends Serializable {
        XContentBuilder getBuilder() throws IOException;
    }
}

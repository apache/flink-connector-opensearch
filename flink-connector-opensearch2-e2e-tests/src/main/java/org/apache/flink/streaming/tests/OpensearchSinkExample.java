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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.opensearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.opensearch.Opensearch2Sink;
import org.apache.flink.streaming.connectors.opensearch.RequestIndexer;
import org.apache.flink.util.Collector;

import org.apache.http.HttpHost;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** End to end test for OpensearchSink. */
public class OpensearchSinkExample {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 2) {
            System.out.println(
                    "Missing parameters!\n" + "Usage: --numRecords <numRecords> --index <index>");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        DataStream<Tuple2<String, String>> source =
                env.generateSequence(0, parameterTool.getInt("numRecords") - 1)
                        .flatMap(
                                new FlatMapFunction<Long, Tuple2<String, String>>() {
                                    @Override
                                    public void flatMap(
                                            Long value, Collector<Tuple2<String, String>> out) {
                                        final String key = String.valueOf(value);
                                        final String message = "message #" + value;
                                        out.collect(Tuple2.of(key, message + "update #1"));
                                        out.collect(Tuple2.of(key, message + "update #2"));
                                    }
                                });

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        Opensearch2Sink.Builder<Tuple2<String, String>> osSinkBuilder =
                new Opensearch2Sink.Builder<>(
                        httpHosts,
                        (Tuple2<String, String> element,
                                RuntimeContext ctx,
                                RequestIndexer indexer) -> {
                            indexer.add(createIndexRequest(element.f1, parameterTool));
                            indexer.add(createUpdateRequest(element, parameterTool));
                        });

        osSinkBuilder.setFailureHandler(
                new CustomFailureHandler(parameterTool.getRequired("index")));

        // this instructs the sink to emit after every element, otherwise they would be buffered
        osSinkBuilder.setBulkFlushMaxActions(1);

        source.addSink(osSinkBuilder.build());

        env.execute("Opensearch end to end sink test example");
    }

    private static class CustomFailureHandler implements ActionRequestFailureHandler {

        private static final long serialVersionUID = 942269087742453482L;

        private final String index;

        CustomFailureHandler(String index) {
            this.index = index;
        }

        @Override
        public void onFailure(
                ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer)
                throws Throwable {
            if (action instanceof IndexRequest) {
                Map<String, Object> json = new HashMap<>();
                json.put("data", ((IndexRequest) action).source());

                indexer.add(
                        Requests.indexRequest()
                                .index(index)
                                .id(((IndexRequest) action).id())
                                .source(json));
            } else {
                throw new IllegalStateException("unexpected");
            }
        }
    }

    private static IndexRequest createIndexRequest(String element, ParameterTool parameterTool) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        String index;
        if (element.startsWith("message #15")) {
            index = ":intentional invalid index:";
        } else {
            index = parameterTool.getRequired("index");
        }

        return Requests.indexRequest().index(index).id(element).source(json);
    }

    private static UpdateRequest createUpdateRequest(
            Tuple2<String, String> element, ParameterTool parameterTool) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element.f1);

        return new UpdateRequest(parameterTool.getRequired("index"), element.f0)
                .doc(json)
                .upsert(json);
    }
}

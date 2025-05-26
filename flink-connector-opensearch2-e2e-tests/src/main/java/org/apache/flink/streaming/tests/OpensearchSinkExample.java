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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.opensearch.sink.FailureHandler;
import org.apache.flink.connector.opensearch.sink.Opensearch2SinkBuilder;
import org.apache.flink.connector.opensearch.sink.OpensearchEmitter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;

import org.apache.http.HttpHost;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Requests;

import java.util.HashMap;
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

        Opensearch2SinkBuilder<Tuple2<String, String>> osSinkBuilder =
                new Opensearch2SinkBuilder<>()
                        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                        .setEmitter(
                                (OpensearchEmitter<Tuple2<String, String>>)
                                        (element, writer, indexer) -> {
                                            indexer.add(
                                                    createIndexRequest(element.f1, parameterTool));
                                            indexer.add(
                                                    createUpdateRequest(element, parameterTool));
                                        })
                        .setFailureHandler(
                                new CustomFailureHandler(parameterTool.getRequired("index")))
                        // this instructs the sink to emit after every element, otherwise they would
                        // be buffered
                        .setBulkFlushMaxActions(1);

        source.sinkTo(osSinkBuilder.build());

        env.execute("Opensearch end to end sink test example");
    }

    private static class CustomFailureHandler implements FailureHandler {

        private static final long serialVersionUID = 942269087742453482L;

        private final String index;

        CustomFailureHandler(String index) {
            this.index = index;
        }

        @Override
        public void onFailure(Throwable failure) {
            throw new IllegalStateException("Unexpected exception for index:" + index);
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

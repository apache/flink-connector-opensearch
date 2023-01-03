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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.opensearch.sink.OpensearchAsyncSink;
import org.apache.flink.connector.opensearch.sink.OpensearchAsyncSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.http.HttpHost;
import org.opensearch.action.index.IndexRequest;

import java.util.ArrayList;
import java.util.List;

/** End to end test for OpensearchAsyncSink. */
public class OpensearchAsyncSinkExample {

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
                env.fromSequence(0, parameterTool.getInt("numRecords") - 1)
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

        OpensearchAsyncSinkBuilder<Tuple2<String, String>> osSinkBuilder =
                OpensearchAsyncSink.<Tuple2<String, String>>builder()
                        .setHosts(new HttpHost("localhost:9200"))
                        .setElementConverter(
                                (element, context) ->
                                        new IndexRequest("my-index")
                                                .id(element.f0.toString())
                                                .source(element.f1));

        source.sinkTo(osSinkBuilder.build());

        env.execute("Opensearch end to end async sink test example");
    }
}

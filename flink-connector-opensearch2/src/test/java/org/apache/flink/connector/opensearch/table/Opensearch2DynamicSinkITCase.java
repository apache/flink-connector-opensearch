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

package org.apache.flink.connector.opensearch.table;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.opensearch.OpensearchUtil;
import org.apache.flink.connector.opensearch.test.DockerImageVersions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchHits;
import org.opensearch.testcontainers.OpensearchContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link OpensearchDynamicSink}. */
@ExtendWith(TestLoggerExtension.class)
@Testcontainers
class Opensearch2DynamicSinkITCase {
    private static final Logger LOG = LoggerFactory.getLogger(Opensearch2DynamicSinkITCase.class);

    @Container
    private static final OpensearchContainer OS_CONTAINER =
            OpensearchUtil.createOpensearchContainer(DockerImageVersions.OPENSEARCH_2, LOG);

    private TestContext getPrefilledTestContext(String index) {
        return TestContext.context()
                .withOption(OpensearchConnectorOptions.INDEX_OPTION.key(), index)
                .withOption(
                        OpensearchConnectorOptions.HOSTS_OPTION.key(),
                        OS_CONTAINER.getHttpHostAddress())
                .withOption(OpensearchConnectorOptions.ALLOW_INSECURE.key(), "true")
                .withOption(
                        OpensearchConnectorOptions.USERNAME_OPTION.key(),
                        OS_CONTAINER.getUsername())
                .withOption(
                        OpensearchConnectorOptions.PASSWORD_OPTION.key(),
                        OS_CONTAINER.getPassword());
    }

    private String getConnectorSql(String index) {
        return String.format("'%s'='%s',\n", "connector", "opensearch-2")
                + String.format(
                        "'%s'='%s',\n", OpensearchConnectorOptions.INDEX_OPTION.key(), index)
                + String.format(
                        "'%s'='%s', \n",
                        OpensearchConnectorOptions.HOSTS_OPTION.key(),
                        OS_CONTAINER.getHttpHostAddress())
                + String.format(
                        "'%s'='%s', \n",
                        OpensearchConnectorOptions.USERNAME_OPTION.key(),
                        OS_CONTAINER.getUsername())
                + String.format(
                        "'%s'='%s', \n",
                        OpensearchConnectorOptions.PASSWORD_OPTION.key(),
                        OS_CONTAINER.getPassword())
                + String.format(
                        "'%s'='%s'\n", OpensearchConnectorOptions.ALLOW_INSECURE.key(), true);
    }

    @Test
    public void testWritingDocuments() throws Exception {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT().notNull()),
                                Column.physical("b", DataTypes.TIME()),
                                Column.physical("c", DataTypes.STRING().notNull()),
                                Column.physical("d", DataTypes.FLOAT()),
                                Column.physical("e", DataTypes.TINYINT().notNull()),
                                Column.physical("f", DataTypes.DATE()),
                                Column.physical("g", DataTypes.TIMESTAMP().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("name", Arrays.asList("a", "g")));
        GenericRowData rowData =
                GenericRowData.of(
                        1L,
                        12345,
                        StringData.fromString("ABCDE"),
                        12.12f,
                        (byte) 2,
                        12345,
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2012-12-12T12:12:12")));

        String index = "writing-documents";
        Opensearch2DynamicSinkFactory sinkFactory = new Opensearch2DynamicSinkFactory();

        DynamicTableSink.SinkRuntimeProvider runtimeProvider =
                sinkFactory
                        .createDynamicTableSink(
                                getPrefilledTestContext(index).withSchema(schema).build())
                        .getSinkRuntimeProvider(new OpensearchUtil.MockContext());

        final SinkV2Provider sinkProvider = (SinkV2Provider) runtimeProvider;
        final Sink<RowData> sink = sinkProvider.createSink();
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);

        rowData.setRowKind(RowKind.UPDATE_AFTER);
        environment.<RowData>fromElements(rowData).sinkTo(sink);
        environment.execute();

        RestHighLevelClient client = OpensearchUtil.createClient(OS_CONTAINER);
        Map<String, Object> response =
                client.get(new GetRequest(index, "1_2012-12-12T12:12:12"), RequestOptions.DEFAULT)
                        .getSource();
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "00:00:12");
        expectedMap.put("c", "ABCDE");
        expectedMap.put("d", 12.12d);
        expectedMap.put("e", 2);
        expectedMap.put("f", "2003-10-20");
        expectedMap.put("g", "2012-12-12 12:12:12");
        assertThat(response).isEqualTo(expectedMap);
    }

    @Test
    public void testWritingDocumentsFromTableApi() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "table-api";
        tableEnvironment.executeSql(
                "CREATE TABLE osTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIME,\n"
                        + "c STRING NOT NULL,\n"
                        + "d FLOAT,\n"
                        + "e TINYINT NOT NULL,\n"
                        + "f DATE,\n"
                        + "g TIMESTAMP NOT NULL,\n"
                        + "h as a + 2,\n"
                        + "PRIMARY KEY (a, g) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + getConnectorSql(index)
                        + ")");

        tableEnvironment
                .fromValues(
                        row(
                                1L,
                                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                                "ABCDE",
                                12.12f,
                                (byte) 2,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("osTable")
                .await();

        RestHighLevelClient client = OpensearchUtil.createClient(OS_CONTAINER);
        Map<String, Object> response =
                client.get(new GetRequest(index, "1_2012-12-12T12:12:12"), RequestOptions.DEFAULT)
                        .getSource();
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "00:00:12");
        expectedMap.put("c", "ABCDE");
        expectedMap.put("d", 12.12d);
        expectedMap.put("e", 2);
        expectedMap.put("f", "2003-10-20");
        expectedMap.put("g", "2012-12-12 12:12:12");
        assertThat(response).isEqualTo(expectedMap);
    }

    @Test
    public void testWritingDocumentsNoPrimaryKey() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "no-primary-key";
        tableEnvironment.executeSql(
                "CREATE TABLE osTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIME,\n"
                        + "c STRING NOT NULL,\n"
                        + "d FLOAT,\n"
                        + "e TINYINT NOT NULL,\n"
                        + "f DATE,\n"
                        + "g TIMESTAMP NOT NULL\n"
                        + ")\n"
                        + "WITH (\n"
                        + getConnectorSql(index)
                        + ")");

        tableEnvironment
                .fromValues(
                        row(
                                1L,
                                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                                "ABCDE",
                                12.12f,
                                (byte) 2,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2012-12-12T12:12:12")),
                        row(
                                2L,
                                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                                "FGHIJK",
                                13.13f,
                                (byte) 4,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2013-12-12T13:13:13")))
                .executeInsert("osTable")
                .await();

        RestHighLevelClient client = OpensearchUtil.createClient(OS_CONTAINER);

        // search API does not return documents that were not indexed, we might need to query
        // the index a few times
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
        SearchHits hits;
        do {
            hits = client.search(new SearchRequest(index), RequestOptions.DEFAULT).getHits();
            if (hits.getTotalHits().value < 2) {
                Thread.sleep(200);
            }
        } while (hits.getTotalHits().value < 2 && deadline.hasTimeLeft());

        if (hits.getTotalHits().value < 2) {
            throw new AssertionError("Could not retrieve results from Opensearch.");
        }

        HashSet<Map<String, Object>> resultSet = new HashSet<>();
        resultSet.add(hits.getAt(0).getSourceAsMap());
        resultSet.add(hits.getAt(1).getSourceAsMap());
        Map<Object, Object> expectedMap1 = new HashMap<>();
        expectedMap1.put("a", 1);
        expectedMap1.put("b", "00:00:12");
        expectedMap1.put("c", "ABCDE");
        expectedMap1.put("d", 12.12d);
        expectedMap1.put("e", 2);
        expectedMap1.put("f", "2003-10-20");
        expectedMap1.put("g", "2012-12-12 12:12:12");
        Map<Object, Object> expectedMap2 = new HashMap<>();
        expectedMap2.put("a", 2);
        expectedMap2.put("b", "00:00:12");
        expectedMap2.put("c", "FGHIJK");
        expectedMap2.put("d", 13.13d);
        expectedMap2.put("e", 4);
        expectedMap2.put("f", "2003-10-20");
        expectedMap2.put("g", "2013-12-12 13:13:13");
        HashSet<Map<Object, Object>> expectedSet = new HashSet<>();
        expectedSet.add(expectedMap1);
        expectedSet.add(expectedMap2);
        assertThat(resultSet).isEqualTo(expectedSet);
    }

    @Test
    public void testWritingDocumentsWithDynamicIndex() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        String index = "dynamic-index-{b|yyyy-MM-dd}";
        tableEnvironment.executeSql(
                "CREATE TABLE osTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIMESTAMP NOT NULL,\n"
                        + "PRIMARY KEY (a) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + getConnectorSql(index)
                        + ")");

        tableEnvironment
                .fromValues(row(1L, LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("osTable")
                .await();

        RestHighLevelClient client = OpensearchUtil.createClient(OS_CONTAINER);
        Map<String, Object> response =
                client.get(new GetRequest("dynamic-index-2012-12-12", "1"), RequestOptions.DEFAULT)
                        .getSource();
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "2012-12-12 12:12:12");
        assertThat(response).isEqualTo(expectedMap);
    }

    @Test
    public void testWritingDocumentsWithDynamicIndexFromSystemTime() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        tableEnvironment
                .getConfig()
                .getConfiguration()
                .setString("table.local-time-zone", "Asia/Shanghai");

        String dynamicIndex1 =
                "dynamic-index-"
                        + dateTimeFormatter.format(LocalDateTime.now(ZoneId.of("Asia/Shanghai")))
                        + "_index";
        String index = "dynamic-index-{now()|yyyy-MM-dd}_index";
        tableEnvironment.executeSql(
                "CREATE TABLE esTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIMESTAMP NOT NULL,\n"
                        + "PRIMARY KEY (a) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + getConnectorSql(index)
                        + ")");
        String dynamicIndex2 =
                "dynamic-index-"
                        + dateTimeFormatter.format(LocalDateTime.now(ZoneId.of("Asia/Shanghai")))
                        + "_index";

        tableEnvironment
                .fromValues(row(1L, LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("esTable")
                .await();

        RestHighLevelClient client = OpensearchUtil.createClient(OS_CONTAINER);

        Map<String, Object> response;
        try {
            response =
                    client.get(new GetRequest(dynamicIndex1, "1"), RequestOptions.DEFAULT)
                            .getSource();
        } catch (OpenSearchStatusException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                response =
                        client.get(new GetRequest(dynamicIndex2, "1"), RequestOptions.DEFAULT)
                                .getSource();
            } else {
                throw e;
            }
        }

        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "2012-12-12 12:12:12");
        assertThat(response).isEqualTo(expectedMap);
    }
}

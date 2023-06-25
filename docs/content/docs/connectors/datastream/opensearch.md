---
title: Opensearch
weight: 5
type: docs
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Opensearch Connector

This connector provides sinks that can request document actions to an
[Opensearch](https://opensearch.org/) Index. To use this connector, add 
the following dependency to your project:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Opensearch version</th>
      <th class="text-left">Maven Dependency</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>1.x</td>
        <td>{{< connector_artifact flink-connector-opensearch opensearch >}}</td>
    </tr>
    <tr>
        <td>2.x</td>
        <td>
          {{< connector_artifact flink-connector-opensearch opensearch >}}

          By default, Apache Flink Opensearch Connector uses 1.3.x client libraries. You could switch to use 2.x (or upcoming 3.x) clients noting that those require **JDK-11 or above**, for example.

          ```xml
          <dependencyManagement>
              <dependencies>
                  <dependency>
                      <groupId>org.opensearch</groupId>
                      <artifactId>opensearch</artifactId>
                      <version>2.5.0</version>
                  </dependency>          
                  <dependency>
                      <groupId>org.opensearch.client</groupId>
                      <artifactId>opensearch-rest-high-level-client</artifactId>
                      <version>2.5.0</version>
                  </dependency>
              </dependencies>
          </dependencyManagement>
          ```
        </td>
    </tr>
  </tbody>
</table>

Note that the streaming connectors are currently not part of the binary
distribution. See [here]({{< ref "docs/dev/configuration/overview" >}}) for information
about how to package the program with the libraries for cluster execution.

## Installing Opensearch

Instructions for setting up an Opensearch cluster can be found
[here](https://opensearch.org/docs/latest/opensearch/install/index/).

## Opensearch Sink

The example below shows how to configure and create a sink:

{{< tabs "a1732edd-4218-470e-adad-b1ebb4021a12" >}}
{{< tab "Java" >}}

```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.http.HttpHost;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

DataStream<String> input = ...;

input.sinkTo(
    new OpensearchSinkBuilder<String>()
        .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
        .setEmitter(
        (element, context, indexer) ->
        indexer.add(createIndexRequest(element)))
        .build());

private static IndexRequest createIndexRequest(String element) {
    Map<String, Object> json = new HashMap<>();
    json.put("data", element);

    return Requests.indexRequest()
        .index("my-index")
        .id(element)
        .source(json);
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.connector.sink.SinkWriter
import org.apache.flink.connector.opensearch.sink.{OpensearchSinkBuilder, RequestIndexer}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.http.HttpHost
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.Requests

val input: DataStream[String] = ...

input.sinkTo(
  new OpensearchSinkBuilder[String]
    .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
    .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
    .setEmitter((element: String, context: SinkWriter.Context, indexer: RequestIndexer) => 
    indexer.add(createIndexRequest(element)))
    .build())

def createIndexRequest(element: (String)): IndexRequest = {

  val json = Map(
    "data" -> element.asInstanceOf[AnyRef]
  )

  Requests.indexRequest.index("my-index").source(mapAsJavaMap(json))
}
```

{{< /tab >}}
{{< /tabs >}}

Note that the example only demonstrates performing a single index
request for each incoming element. Generally, the `OpensearchEmitter`
can be used to perform requests of different types (ex.,
`DeleteRequest`, `UpdateRequest`, etc.). 

Internally, each parallel instance of the Flink Opensearch Sink uses
a `BulkProcessor` to send action requests to the cluster.
This will buffer elements before sending them in bulk to the cluster. The `BulkProcessor`
executes bulk requests one at a time, i.e. there will be no two concurrent
flushes of the buffered actions in progress.

### Opensearch Sinks and Fault Tolerance

With Flink’s checkpointing enabled, the Flink Opensearch Sink guarantees
at-least-once delivery of action requests to Opensearch clusters. It does
so by waiting for all pending action requests in the `BulkProcessor` at the
time of checkpoints. This effectively assures that all requests before the
checkpoint was triggered have been successfully acknowledged by Opensearch, before
proceeding to process more records sent to the sink.

More details on checkpoints and fault tolerance are in the [fault tolerance docs]({{< ref "docs/learn-flink/fault_tolerance" >}}).

To use fault tolerant Opensearch Sinks, checkpointing of the topology needs to be enabled at the execution environment:

{{< tabs "aa0d1e93-4844-40d7-b0ec-9ec37e731a5f" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // checkpoint every 5000 msecs
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // checkpoint every 5000 msecs
```

{{< /tab >}}
{{< /tabs >}}

<p style="border-radius: 5px; padding: 5px" class="bg-info">
<b>IMPORTANT</b>: Checkpointing is not enabled by default but the default delivery guarantee is `AT_LEAST_ONCE`.
This causes the sink to buffer requests until it either finishes or the `BulkProcessor` flushes automatically. 
By default, the `BulkProcessor` will flush after `1000` added actions. To configure the processor to flush more frequently, please refer to the <a href="#configuring-the-internal-bulk-processor">BulkProcessor configuration section</a>.
</p>

<p style="border-radius: 5px; padding: 5px" class="bg-info">
Using `UpdateRequests` with deterministic IDs and the upsert method it is possible to achieve exactly-once semantics in Opensearch when `AT_LEAST_ONCE` delivery is configured for the connector.
</p>

### Handling Failing Opensearch Requests

Opensearch action requests may fail due to a variety of reasons, including
temporarily saturated node queue capacity or malformed documents to be indexed.
The Flink Opensearch Sink allows the user to retry requests by specifying a backoff-policy.

Below is an example:

{{< tabs "adb958b3-5dd5-476e-b946-ace3335628ea" >}}
{{< tab "Java" >}}
```java
DataStream<String> input = ...;

input.sinkTo(
    new OpensearchSinkBuilder<String>()
        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
        .setEmitter(
        (element, context, indexer) ->
        indexer.add(createIndexRequest(element)))
        // This enables an exponential backoff retry mechanism, with a maximum of 5 retries and an initial delay of 1000 milliseconds
        .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000)
        .build());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[String] = ...

input.sinkTo(
  new OpensearchSinkBuilder[String]
    .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
    .setEmitter((element: String, context: SinkWriter.Context, indexer: RequestIndexer) => 
    indexer.add(createIndexRequest(element)))
    // This enables an exponential backoff retry mechanism, with a maximum of 5 retries and an initial delay of 1000 milliseconds
    .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000)
    .build())
```

{{< /tab >}}
{{< /tabs >}}

The above example will let the sink re-add requests that failed due to resource constrains (e.g.
queue capacity saturation). For all other failures, such as malformed documents, the sink will fail. 
If no `BulkFlushBackoffStrategy` (or `FlushBackoffType.NONE`) is configured, the sink will fail for any kind of error.

<p style="border-radius: 5px; padding: 5px" class="bg-danger">
<b>IMPORTANT</b>: Re-adding requests back to the internal <b>BulkProcessor</b>
on failures will lead to longer checkpoints, as the sink will also
need to wait for the re-added requests to be flushed when checkpointing.
For example, when using <b>FlushBackoffType.EXPONENTIAL</b>, checkpoints
will need to wait until Opensearch node queues have enough capacity for
all the pending requests, or until the maximum number of retries has been reached.
</p>

### Configuring the Internal Bulk Processor

The internal `BulkProcessor` can be further configured for its behaviour
on how buffered action requests are flushed, by using the following methods of the OpensearchSinkBuilder:

* **setBulkFlushMaxActions(int numMaxActions)**: Maximum amount of actions to buffer before flushing.
* **setBulkFlushMaxSizeMb(int maxSizeMb)**: Maximum size of data (in megabytes) to buffer before flushing.
* **setBulkFlushInterval(long intervalMillis)**: Interval at which to flush regardless of the amount or size of buffered actions.
 
Configuring how temporary request errors are retried is also supported:
 * **setBulkFlushBackoffStrategy(FlushBackoffType flushBackoffType, int maxRetries, long delayMillis)**: The type of backoff delay, either `CONSTANT` or `EXPONENTIAL`, the amount of backoff retries to attempt, the amount of delay for backoff. For constant backoff, this
   is simply the delay between each retry. For exponential backoff, this is the initial base delay.

More information about Opensearch can be found [here](https://opensearch.org/).

## Packaging the Opensearch Connector into an Uber-Jar

For the execution of your Flink program, it is recommended to build a
so-called uber-jar (executable jar) containing all your dependencies
(see [here]({{< ref "docs/dev/configuration/overview" >}}) for further information).

Alternatively, you can put the connector's jar file into Flink's `lib/` folder to make it available
system-wide, i.e. for all job being run.

{{< top >}}

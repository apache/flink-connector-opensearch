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

import org.apache.flink.annotation.PublicEvolving;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Wrapper class around {@link DocWriteRequest} since it does not implement {@link Serializable},
 * required by AsyncSink scaffolding.
 */
@PublicEvolving
public class DocSerdeRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    private final DocWriteRequest<?> request;

    private DocSerdeRequest(DocWriteRequest<?> request) {
        this.request = request;
    }

    public DocWriteRequest<?> getRequest() {
        return request;
    }

    static <T> DocSerdeRequest from(DocWriteRequest<T> request) {
        return new DocSerdeRequest(request);
    }

    static DocSerdeRequest readFrom(long requestSize, DataInputStream in) throws IOException {
        try (final StreamInput stream = new InputStreamStreamInput(in, requestSize)) {
            return new DocSerdeRequest(readDocumentRequest(stream));
        }
    }

    void writeTo(DataOutputStream out) throws IOException {
        try (BytesStreamOutput stream = new BytesStreamOutput()) {
            writeDocumentRequest(stream, request);
            out.write(BytesReference.toBytes(stream.bytes()));
        }
    }

    /** Read a document write (index/delete/update) request. */
    private static DocWriteRequest<?> readDocumentRequest(StreamInput in) throws IOException {
        byte type = in.readByte();
        DocWriteRequest<?> docWriteRequest;
        if (type == 0) {
            docWriteRequest = new IndexRequest(in);
        } else if (type == 1) {
            docWriteRequest = new DeleteRequest(in);
        } else if (type == 2) {
            docWriteRequest = new UpdateRequest(in);
        } else {
            throw new IllegalStateException("Invalid request type [" + type + " ]");
        }
        return docWriteRequest;
    }

    /** Write a document write (index/delete/update) request. */
    private static void writeDocumentRequest(StreamOutput out, DocWriteRequest<?> request)
            throws IOException {
        if (request instanceof IndexRequest) {
            out.writeByte((byte) 0);
            ((IndexRequest) request).writeTo(out);
        } else if (request instanceof DeleteRequest) {
            out.writeByte((byte) 1);
            ((DeleteRequest) request).writeTo(out);
        } else if (request instanceof UpdateRequest) {
            out.writeByte((byte) 2);
            ((UpdateRequest) request).writeTo(out);
        } else {
            throw new IllegalStateException(
                    "Invalid request [" + request.getClass().getSimpleName() + " ]");
        }
    }
}

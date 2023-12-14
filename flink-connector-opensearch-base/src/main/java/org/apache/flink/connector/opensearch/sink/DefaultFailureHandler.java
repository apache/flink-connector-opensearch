package org.apache.flink.connector.opensearch.sink;

import org.apache.flink.util.FlinkRuntimeException;

class DefaultFailureHandler implements FailureHandler {

    @Override
    public void onFailure(Throwable failure) {
        if (failure instanceof FlinkRuntimeException) {
            throw (FlinkRuntimeException) failure;
        }
        throw new FlinkRuntimeException(failure);
    }
}

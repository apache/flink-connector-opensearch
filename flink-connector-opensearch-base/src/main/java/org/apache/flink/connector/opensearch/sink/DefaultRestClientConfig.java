package org.apache.flink.connector.opensearch.sink;

import javax.annotation.Nullable;

import java.util.Optional;

/** Provides the default implementation for {@link RestClientFactory.RestClientConfig}. */
class DefaultRestClientConfig implements RestClientFactory.RestClientConfig {
    private final NetworkClientConfig networkClientConfig;

    DefaultRestClientConfig(NetworkClientConfig networkClientConfig) {
        this.networkClientConfig = networkClientConfig;
    }

    @Override
    public @Nullable String getUsername() {
        return networkClientConfig.getUsername();
    }

    @Override
    public @Nullable String getPassword() {
        return networkClientConfig.getPassword();
    }

    @Override
    public @Nullable Integer getConnectionRequestTimeout() {
        return networkClientConfig.getConnectionRequestTimeout();
    }

    @Override
    public @Nullable Integer getConnectionTimeout() {
        return networkClientConfig.getConnectionTimeout();
    }

    @Override
    public @Nullable Integer getSocketTimeout() {
        return networkClientConfig.getSocketTimeout();
    }

    @Override
    public @Nullable String getConnectionPathPrefix() {
        return networkClientConfig.getConnectionPathPrefix();
    }

    @Override
    public Optional<Boolean> isAllowInsecure() {
        return networkClientConfig.isAllowInsecure();
    }
}

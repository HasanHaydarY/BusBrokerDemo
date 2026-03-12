package com.troia.bus.kafka;

import com.troia.bus.core.BusConnector;
import com.troia.bus.core.BusConnection;

public class KafkaConnector implements BusConnector {

    static final long DEFAULT_CONFIRM_TIMEOUT_MS = 30_000;

    private final String bootstrapServers;
    private final String serviceName;
    private final String groupId;
    private final long   confirmTimeoutMs;

    public KafkaConnector(String bootstrapServers, String serviceName,
                          String groupId, long confirmTimeoutMs) {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty())
            throw new IllegalArgumentException("bootstrapServers cannot be blank");
        if (serviceName == null || serviceName.trim().isEmpty())
            throw new IllegalArgumentException("serviceName cannot be blank");
        if (groupId == null || groupId.trim().isEmpty())
            throw new IllegalArgumentException("groupId cannot be blank");
        if (confirmTimeoutMs <= 0)
            throw new IllegalArgumentException("confirmTimeoutMs must be > 0");

        this.bootstrapServers = bootstrapServers;
        this.serviceName      = serviceName;
        this.groupId          = groupId;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    public KafkaConnector(String bootstrapServers, String serviceName, String groupId) {
        this(bootstrapServers, serviceName, groupId, DEFAULT_CONFIRM_TIMEOUT_MS);
    }

    @Override
    public BusConnection connect() throws Exception {
        return new KafkaConnection(bootstrapServers, serviceName, groupId, confirmTimeoutMs);
    }

    @Override
    public BusConnection connectWithTimeout(long confirmTimeoutMs) throws Exception {
        return new KafkaConnection(bootstrapServers, serviceName, groupId, confirmTimeoutMs);
    }

    @Override
    public long defaultConfirmTimeoutMs() {
        return confirmTimeoutMs;
    }

    @Override
    public String brokerType() {
        return "kafka";
    }
}
package com.troia.bus.redpanda;

import com.troia.bus.core.BusConnector;
import com.troia.bus.core.BusConnection;
import com.troia.bus.kafka.KafkaConnection;

import java.util.Properties;

public class RedpandaConnector implements BusConnector {

    private final String     bootstrapServers;
    private final String     serviceName;
    private final String     groupId;
    private final Properties producerOverrides;

    public RedpandaConnector(String bootstrapServers, String serviceName,
                             String groupId, Properties producerOverrides) {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty())
            throw new IllegalArgumentException("bootstrapServers cannot be blank");
        if (serviceName == null || serviceName.trim().isEmpty())
            throw new IllegalArgumentException("serviceName cannot be blank");
        if (groupId == null || groupId.trim().isEmpty())
            throw new IllegalArgumentException("groupId cannot be blank");

        this.bootstrapServers  = bootstrapServers;
        this.serviceName       = serviceName;
        this.groupId           = groupId;
        this.producerOverrides = producerOverrides != null ? producerOverrides : new Properties();
    }

    public RedpandaConnector(String bootstrapServers, String serviceName, String groupId) {
        this(bootstrapServers, serviceName, groupId, null);
    }

    @Override
    public BusConnection connect() throws Exception {
        return new KafkaConnection(bootstrapServers, serviceName, groupId, producerOverrides);
    }

    @Override
    public String brokerType() {
        return "redpanda";
    }
}
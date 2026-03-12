package com.troia.bus.rabbit;

import com.troia.bus.core.BusConnector;
import com.troia.bus.core.BusConnection;

public class RabbitConnector implements BusConnector {

    static final long DEFAULT_CONFIRM_TIMEOUT_MS = 10_000;

    private final String host;
    private final int    port;
    private final String username;
    private final String password;
    private final String vhost;
    private final String serviceName;
    private final long   confirmTimeoutMs;

    public RabbitConnector(String host, int port,
                           String username, String password,
                           String vhost, String serviceName,
                           long confirmTimeoutMs) {
        if (host == null || host.trim().isEmpty())
            throw new IllegalArgumentException("host cannot be blank");
        if (username == null || username.trim().isEmpty())
            throw new IllegalArgumentException("username cannot be blank");
        if (vhost == null || vhost.trim().isEmpty())
            throw new IllegalArgumentException("vhost cannot be blank");
        if (serviceName == null || serviceName.trim().isEmpty())
            throw new IllegalArgumentException("serviceName cannot be blank");
        if (confirmTimeoutMs <= 0)
            throw new IllegalArgumentException("confirmTimeoutMs must be > 0");

        this.host             = host;
        this.port             = port;
        this.username         = username;
        this.password         = password;
        this.vhost            = vhost;
        this.serviceName      = serviceName;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    public RabbitConnector(String host, int port,
                           String username, String password,
                           String vhost, String serviceName) {
        this(host, port, username, password, vhost, serviceName, DEFAULT_CONFIRM_TIMEOUT_MS);
    }

    @Override
    public BusConnection connect() throws Exception {
        return new RabbitConnection(host, port, username, password, vhost, serviceName, confirmTimeoutMs);
    }

    @Override
    public BusConnection connectWithTimeout(long confirmTimeoutMs) throws Exception {
        return new RabbitConnection(host, port, username, password, vhost, serviceName, confirmTimeoutMs);
    }

    @Override
    public long defaultConfirmTimeoutMs() {
        return confirmTimeoutMs;
    }

    @Override
    public String brokerType() {
        return "rabbit";
    }
}
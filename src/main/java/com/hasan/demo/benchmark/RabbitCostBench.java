package com.hasan.demo.benchmark;

import com.rabbitmq.client.*;
import org.openjdk.jmh.annotations.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
@State(Scope.Benchmark)
public class RabbitCostBench {

    public static final String HOST = "localhost";
    public static final String EXCHANGE = "ex.agents";
    public static final String EXCHANGE_TYPE = "direct";

    // ✅ Her benchmark ayrı routing key + ayrı queue
    public static final String RK_A = "bench.a";
    public static final String RK_B = "bench.b";
    public static final String RK_C = "bench.c";

    public static final String QUEUE_A = "q.bench.a";
    public static final String QUEUE_B = "q.bench.b";
    public static final String QUEUE_C = "q.bench.c";

    public static final byte[] BODY = "hello".getBytes(StandardCharsets.UTF_8);

    ConnectionFactory factory;

    Connection sharedConn;
    Channel sharedCh;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        factory = new ConnectionFactory();
        factory.setHost(HOST);

        sharedConn = factory.newConnection();
        sharedCh = sharedConn.createChannel();

        // ✅ Exchange
        sharedCh.exchangeDeclare(EXCHANGE, EXCHANGE_TYPE, true);

        // ✅ Queue + bind (A/B/C ayrı ayrı)
        sharedCh.queueDeclare(QUEUE_A, true, false, false, null);
        sharedCh.queueDeclare(QUEUE_B, true, false, false, null);
        sharedCh.queueDeclare(QUEUE_C, true, false, false, null);

        sharedCh.queueBind(QUEUE_A, EXCHANGE, RK_A);
        sharedCh.queueBind(QUEUE_B, EXCHANGE, RK_B);
        sharedCh.queueBind(QUEUE_C, EXCHANGE, RK_C);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws Exception {
        if (sharedCh != null && sharedCh.isOpen()) sharedCh.close();
        if (sharedConn != null && sharedConn.isOpen()) sharedConn.close();
    }

    // A) Her çağrıda connection+channel+publish
    @Benchmark
    public void conn_and_channel_each_time() throws Exception {
        try (Connection conn = factory.newConnection();
             Channel ch = conn.createChannel()) {

            ch.basicPublish(EXCHANGE, RK_A, null, BODY);
        }
    }

    // B) Shared connection, her çağrıda yeni channel
    @Benchmark
    public void channel_each_time_shared_connection() throws Exception {
        Channel ch = sharedConn.createChannel();
        try {
            ch.basicPublish(EXCHANGE, RK_B, null, BODY);
        } finally {
            if (ch.isOpen()) ch.close();
        }
    }

    // C) Shared connection + shared channel, sadece publish
    @Benchmark
    public void publish_only_shared_channel() throws Exception {
        sharedCh.basicPublish(EXCHANGE, RK_C, null, BODY);
    }
}
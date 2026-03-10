package com.troia.bus.core;

import java.util.UUID;

public class Envelope {

    public String messageId;
    public String correlationId;
    public long   timestampEpochMs;

    public String type;
    public String source;
    public String target;
    public int    version = 1;
    public Object payload;

    public static Envelope of(String type, String source, Object payload) {
        Envelope e = new Envelope();
        e.messageId       = UUID.randomUUID().toString();
        e.timestampEpochMs = System.currentTimeMillis();
        e.type    = type;
        e.source  = source;
        e.payload = payload;
        return e;
    }
}
package com.hasan.bus.core;

import java.util.UUID;

public class Envelope {
	public String messageId;
	public String correlationId; // command-reply / trace
	public long timestampEpochMs;

	public String type; // "event.telemetry" / "command.setTag"
	public String source; // "agent-7"
	public String target; // "app-service" / "agent-7" (opsiyonel)

	public int version = 1;

	public Object payload; // Jackson bunu JSON’a çevirir

	public static Envelope of(String type, String source, Object payload) {
		Envelope e = new Envelope();
		e.messageId = UUID.randomUUID().toString();
		e.timestampEpochMs = System.currentTimeMillis();
		e.type = type;
		e.source = source;
		e.payload = payload;
		return e;
	}
}

package com.hasan.bus.core;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonCodec {
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private JsonCodec() {
	}

	public static String toJson(Object o) throws Exception {
		return MAPPER.writeValueAsString(o);
	}

	public static <T> T fromJson(String json, Class<T> cls) throws Exception {
		return MAPPER.readValue(json, cls);
	}
}

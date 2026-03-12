package com.troia.bus.core;

import java.util.List;

public class PollResult {

	public final List<Envelope> messages;
	public final List<String> failed;

	public PollResult(List<Envelope> messages, List<String> failed) {
		this.messages = messages;
		this.failed = failed;
	}

	public boolean hasMessages() {
		return !messages.isEmpty();
	}

	public boolean hasFailed() {
		return !failed.isEmpty();
	}
}
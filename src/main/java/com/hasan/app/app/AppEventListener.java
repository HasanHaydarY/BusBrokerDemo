package com.hasan.app.app;

import com.hasan.bus.core.Envelope;
import com.hasan.bus.core.MessageHandler;

public class AppEventListener implements MessageHandler {

    @Override
    public boolean onMessage(Envelope env) {
        System.out.println("[APP] CONSUMED type=" + env.type
                + " source=" + env.source
                + " payload=" + env.payload);

        return true;
    }
}

package com.hasan.app.iot;

import com.hasan.bus.core.Envelope;
import com.hasan.bus.core.MessageHandler;

public class IotEventListener implements MessageHandler {

    @Override
    public boolean onMessage(Envelope env) {
        System.out.println("[IOT] CONSUMED type=" + env.type
                + " source=" + env.source
                + " payload=" + env.payload);

        // Şimdilik hep başarılı sayıyoruz (ACK)
        return true;
    }
}

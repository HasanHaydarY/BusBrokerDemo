package com.hasan.bus.core;

/**
 * Tüm broker implementasyonları için ortak temel sınıf.
 *
 */
public abstract class BaseBus implements MessageBus {

    private volatile boolean open = false;

    @Override
    public final void open() throws Exception {
        if (open) {
            throw new IllegalStateException("Bus is already open. Call close() first.");
        }
        doOpen();
        open = true;
    }

    @Override
    public final void close() throws Exception {
        if (!open) {
            return;
        }
        try {
            doClose();
        } finally {
            open = false;
        }
    }

    protected abstract void doOpen() throws Exception;
    protected abstract void doClose() throws Exception;

    protected void requireOpen() {
        if (!open) {
            throw new IllegalStateException("Bus is not open. Call open() first.");
        }
    }
}
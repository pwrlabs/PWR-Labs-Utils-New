package io.pwrlabs.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A one‑shot event barrier that automatically resets after each trigger.
 *
 * awaitEvent() will block until triggerEvent() is called.
 * triggerEvent() releases all current waiters and then resets for the next round.
 */
public class TriggerEvent {
    // Always points to the latch for the *current* cycle
    private final AtomicReference<CountDownLatch> latchRef =
            new AtomicReference<>(new CountDownLatch(1));

    /**
     * Blocks until triggerEvent() is called.
     */
    public void awaitEvent() throws InterruptedException {
        latchRef.get().await();
    }

    /**
     * Blocks until triggerEvent() is called, or the timeout elapses.
     * @return true if released by triggerEvent(), false if timed out
     */
    public boolean awaitEvent(long timeout, TimeUnit unit) throws InterruptedException {
        return latchRef.get().await(timeout, unit);
    }

    /**
     * Releases all waiting threads, then immediately resets
     * so that subsequent calls to awaitEvent() will block again.
     */
    public void triggerEvent() {
        // swap in a brand‑new latch for the next cycle
        CountDownLatch old = latchRef.getAndSet(new CountDownLatch(1));
        // countDown the *old* latch, releasing everyone who was waiting on it
        old.countDown();
    }
}


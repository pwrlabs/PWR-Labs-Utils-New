package io.pwrlabs.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

public class PWRReentrantReadWriteLock {
    //region ==================== Fields ========================
    private ReadWriteLock readWriteLock;

    private AtomicInteger readLockCount = new AtomicInteger(0);

    private Thread writeLockThread = null;
    private long writeLockTime = 0; //Time in nanoseconds
    //endregion

    //region ==================== Constructor ========================
    public PWRReentrantReadWriteLock() {
        this.readWriteLock = new java.util.concurrent.locks.ReentrantReadWriteLock();
    }
    //endregion

    //region ==================== Public Getters ========================
    /**
     * @return The thread that currently holds the write lock*/
    public Thread getWriteLockThread() {
        return writeLockThread;
    }

    /**
     * @return The time when the write lock was last acquired in nano seconds. Or 0 if the lock is not currently held by any thread
     * */
    public long getWriteLockTime() {
        return writeLockTime;
    }
    //endregion

    //region ==================== Public Methods ========================
    public void acquireReadLock() {
        readWriteLock.readLock().lock();
        readLockCount.incrementAndGet();
    }

    public void releaseReadLock() {
        readWriteLock.readLock().unlock();
        readLockCount.decrementAndGet();
    }

    public void acquireWriteLock() {
        readWriteLock.writeLock().lock();
        writeLockThread = Thread.currentThread();
        if(writeLockTime == 0) writeLockTime = System.nanoTime();
    }

    public void releaseWriteLock() {
        if(writeLockThread != Thread.currentThread()) {
            throw new IllegalStateException("Current thread does not hold the write lock");
        } else {
            writeLockThread = null;
            writeLockTime = 0;
            readWriteLock.writeLock().unlock();
        }
    }
    //endregion


}

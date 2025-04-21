package io.pwrlabs.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.pwrlabs.newerror.NewError.errorIf;

/**
 * A reentrant read-write lock implementation that extends Java's standard {@link ReadWriteLock} capabilities
 * with additional tracking functionality. This class keeps track of the number of active read locks,
 * the thread holding the write lock, and the timestamp when the write lock was acquired.
 *
 * <p>This implementation maintains the same thread safety and reentrant properties as the standard
 * {@link java.util.concurrent.locks.ReentrantReadWriteLock} while providing additional information
 * about lock state and ownership.</p>
 *
 * <p>The lock has two modes:</p>
 * <ul>
 *   <li>Read mode: Multiple threads can hold the read lock simultaneously</li>
 *   <li>Write mode: Only one thread can hold the write lock, excluding all read locks</li>
 * </ul>
 */
public class PWRReentrantReadWriteLock {
    //region ==================== Fields ========================
    /**
     * The underlying read-write lock implementation
     */
    private ReadWriteLock readWriteLock;

    private ReadWriteLock fieldsLock = new ReentrantReadWriteLock();

    /**
     * Counter tracking the number of active read locks
     */
    private AtomicInteger readLockCount = new AtomicInteger(0);
    private AtomicInteger writeLockCount = new AtomicInteger(0);

    /**
     * Reference to the thread currently holding the write lock, or null if no thread holds it
     */
    private Thread writeLockThread = null;

    /**
     * Timestamp in nanoseconds when the write lock was acquired
     * Value is 0 when no thread holds the write lock
     */
    private long writeLockTime = 0; //Time in nanoseconds
    //endregion

    //region ==================== Constructor ========================

    /**
     * Creates a new PWRReentrantReadWriteLock instance.
     * Initializes the underlying read-write lock as a {@link java.util.concurrent.locks.ReentrantReadWriteLock}.
     */
    protected PWRReentrantReadWriteLock() {
        this.readWriteLock = new ReentrantReadWriteLock();
    }
    //endregion

    //region ==================== Public Getters ========================

    /**
     * Returns the thread that currently holds the write lock.
     *
     * @return The thread that currently holds the write lock, or null if the write lock is not held
     */
    public Thread getWriteLockThread() {
        fieldsLock.readLock().lock();
        try {
            return writeLockThread;
        } finally {
            fieldsLock.readLock().unlock();
        }
    }

    /**
     * Returns the timestamp when the write lock was last acquired.
     *
     * @return The time when the write lock was last acquired in nanoseconds,
     * or 0 if the lock is not currently held by any thread
     */
    public long getWriteLockTime() {
        fieldsLock.readLock().lock();
        try {
            return writeLockTime;
        } finally {
            fieldsLock.readLock().unlock();
        }
    }

    /**
     * Checks if the write lock is held by the current thread.
     *
     * @return true if the write lock is held by the current thread, false otherwise
     */
    public boolean isHeldByCurrentThread() {
        fieldsLock.readLock().lock();
        try {
            return writeLockThread == Thread.currentThread();
        } finally {
            fieldsLock.readLock().unlock();
        }
    }
    //endregion

    //region ==================== Public Methods ========================

    /**
     * Acquires the read lock.
     *
     * <p>Multiple threads can hold the read lock simultaneously, but the read lock
     * cannot be acquired if any thread holds the write lock.</p>
     *
     * <p>This method will block until the read lock can be acquired.</p>
     *
     * <p>After this method returns successfully, the read lock count is incremented.</p>
     */
    public void acquireReadLock() {
        readWriteLock.readLock().lock();
        readLockCount.incrementAndGet();
    }

    /**
     * Releases the read lock.
     *
     * <p>This method decrements the read lock count and releases the underlying read lock.</p>
     *
     * <p>This method should only be called by a thread that holds the read lock,
     * otherwise the underlying lock implementation may throw an exception.</p>
     */
    public void releaseReadLock() {
        readWriteLock.readLock().unlock();
        readLockCount.decrementAndGet();
    }

    /**
     * Acquires the write lock.
     *
     * <p>The write lock can only be held by a single thread at a time and is exclusive
     * with all read locks. This method will block until the write lock can be acquired.</p>
     *
     * <p>After this method returns successfully:</p>
     * <ul>
     *   <li>The current thread is set as the write lock holder</li>
     *   <li>If this is the first acquisition (not a reentrant acquisition), the write lock
     *       timestamp is updated to the current time in nanoseconds</li>
     * </ul>
     */
    public void acquireWriteLock() {
        readWriteLock.writeLock().lock();

        if(writeLockThread == null) {
            writeLockThread = Thread.currentThread();
            writeLockTime = System.nanoTime();
        }

        writeLockCount.incrementAndGet();
    }

    /**
     * Releases the write lock.
     *
     * <p>This method releases the write lock and clears the associated tracking information.</p>
     *
     * @throws IllegalStateException if the current thread does not hold the write lock
     */
    public void releaseWriteLock() {
        errorIf(writeLockThread == null, "Write lock is not held by any thread");
        errorIf(writeLockThread != Thread.currentThread(), "Current thread does not hold the write lock");

        if (writeLockCount.decrementAndGet() == 0) {
            writeLockThread = null;
            writeLockTime = 0;
        }

        readWriteLock.writeLock().unlock();
    }

    public boolean tryToAcquireWriteLock() {
        if (readWriteLock.writeLock().tryLock()) {
            if (writeLockThread == null) {
                writeLockThread = Thread.currentThread();
                writeLockTime = System.nanoTime();
            }
            writeLockCount.incrementAndGet();
            return true;
        }
        return false;
    }
    //endregion

    //region ==================== Private Methods ========================
    private void setWriteLockThread(Thread thread) {
        errorIf(thread == null, "Thread cannot be null");
        fieldsLock.writeLock().lock();
        try {
            this.writeLockThread = thread;
        } finally {
            fieldsLock.writeLock().unlock();
        }
    }

    private void setWriteLockTime(long time) {
        errorIf(time < 0, "Time cannot be negative");
        fieldsLock.writeLock().lock();
        try {
            this.writeLockTime = time;
        } finally {
            fieldsLock.writeLock().unlock();
        }
    }
    //endregion
}
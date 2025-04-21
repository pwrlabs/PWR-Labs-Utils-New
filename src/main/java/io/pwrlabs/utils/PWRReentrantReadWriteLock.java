package io.pwrlabs.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

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

    /**
     * Counter tracking the number of active read locks
     */
    private AtomicInteger readLockCount = new AtomicInteger(0);

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
        this.readWriteLock = new java.util.concurrent.locks.ReentrantReadWriteLock();
    }
    //endregion

    //region ==================== Public Getters ========================
    /**
     * Returns the thread that currently holds the write lock.
     *
     * @return The thread that currently holds the write lock, or null if the write lock is not held
     */
    public Thread getWriteLockThread() {
        return writeLockThread;
    }

    /**
     * Returns the timestamp when the write lock was last acquired.
     *
     * @return The time when the write lock was last acquired in nanoseconds,
     *         or 0 if the lock is not currently held by any thread
     */
    public long getWriteLockTime() {
        return writeLockTime;
    }

    //isHeldByCurrentThread
    /**
     * Checks if the write lock is held by the current thread.
     *
     * @return true if the write lock is held by the current thread, false otherwise
     */
    public boolean isHeldByCurrentThread() {
        return writeLockThread == Thread.currentThread();
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
        writeLockThread = Thread.currentThread();
        if(writeLockTime == 0) writeLockTime = System.nanoTime();
    }

    /**
     * Releases the write lock.
     *
     * <p>This method releases the write lock and clears the associated tracking information.</p>
     *
     * @throws IllegalStateException if the current thread does not hold the write lock
     */
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
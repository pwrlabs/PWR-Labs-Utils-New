package io.pwrlabs.concurrency;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentHashSet<T> {
    private final Set<T> set = new HashSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConcurrentHashSet() {
        // Default constructor
    }

    public void add(T element) {
        lock.writeLock().lock();
        try {
            set.add(element);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(T element) {
        lock.writeLock().lock();
        try {
            set.remove(element);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes any elements that do not match the condition*/
    public void removeIf(java.util.function.Predicate<? super T> filter) {
        lock.writeLock().lock();
        try {
            set.removeIf(filter);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean contains(T element) {
        lock.readLock().lock();
        try {
            return set.contains(element);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<T> getSetCopy() {
        lock.readLock().lock();
        try {
            return new HashSet<>(set);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return set.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            set.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}

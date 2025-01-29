package io.pwrlabs.concurrency;

import lombok.Getter;

import java.lang.ref.Cleaner;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentMap<K, V> implements AutoCloseable {
    @Getter
    private static final AtomicInteger mapCount = new AtomicInteger(0);
    private static final Cleaner CLEANER = Cleaner.create();
    private final Cleaner.Cleanable cleanable;

    private final Map<K, V> map;
    private final ReadWriteLock lock;

    public ConcurrentMap() { 

        this.map = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();

        cleanable = CLEANER.register(this, new CleaningAction());
        mapCount.incrementAndGet();
    }

    public ConcurrentMap(int mapSize ) {        this.map = new HashMap<>(mapSize);
        this.lock = new ReentrantReadWriteLock();

        cleanable = CLEANER.register(this, new CleaningAction());
        mapCount.incrementAndGet();
    }

    public ConcurrentMap(ConcurrentMap<K, V> ConcurrentMap) { 
        this.map = new HashMap<>(ConcurrentMap.map);
        this.lock = new ReentrantReadWriteLock();

        cleanable = CLEANER.register(this, new CleaningAction());
        mapCount.incrementAndGet();
    }

    public ConcurrentMap(Map<K, V> map) { 
        this.map = new HashMap<>(map);
        this.lock = new ReentrantReadWriteLock();

        cleanable = CLEANER.register(this, new CleaningAction());
        mapCount.incrementAndGet();
    }

    public void put(K key, V value ) {        lock.writeLock().lock();
        try {
            map.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(K key) { 
        lock.writeLock().lock();
        try {
            map.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public V get(K key) { 
        lock.readLock().lock();
        try {
            return map.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public V getOrDefault(K key, V defaultValue ) {        lock.readLock().lock();
        try {
            return map.getOrDefault(key, defaultValue);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean containsKey(K key) { 
        lock.readLock().lock();
        try {
            return map.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<K> keySet() { 
        lock.readLock().lock();
        try {
            return new ArrayList<>(map.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }
    public List<V> values() { 
        lock.readLock().lock();
        try {
            return new ArrayList<>(map.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<Map.Entry<K, V>> entrySet() { 
        lock.readLock().lock();
        try {
            return map.entrySet();
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return map.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            map.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    private static class CleaningAction implements Runnable {
        @Override
        public void run() {
            mapCount.decrementAndGet();
        }
    }
}

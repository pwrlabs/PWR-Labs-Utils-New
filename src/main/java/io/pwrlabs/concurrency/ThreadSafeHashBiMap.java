package io.pwrlabs.concurrency;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A thread-safe bidirectional map implementation.
 * Maintains two maps that allow lookups in either direction.
 * Values must be unique, similar to keys.
 *
 * @param <K> The type of the keys
 * @param <V> The type of the values
 */
public class ThreadSafeHashBiMap<K, V> {

    private final Map<K, V> forward = new ConcurrentHashMap<>();
    private final Map<V, K> backward = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Creates a new empty ThreadSafeHashBiMap.
     */
    public ThreadSafeHashBiMap() {
        // Initialize with empty concurrent maps
    }

    /**
     * Associates the specified key with the specified value in this map.
     * If the map previously contained a mapping for the key or value, the old mapping is replaced.
     *
     * @param key The key with which the specified value is to be associated
     * @param value The value to be associated with the specified key
     * @return The previous value associated with the key, or null if there was no mapping
     * @throws IllegalArgumentException if the value is already mapped to another key
     */
    public V put(K key, V value) {
        lock.writeLock().lock();
        try {
            // Check if the value is already mapped to a different key
            K existingKey = backward.get(value);
            if (existingKey != null && !existingKey.equals(key)) {
                throw new IllegalArgumentException("Value already exists in the map with a different key");
            }

            // Remove previous mapping for this key if it exists
            V oldValue = forward.get(key);
            if (oldValue != null) {
                backward.remove(oldValue);
            }

            // Add the new mappings
            forward.put(key, value);
            backward.put(value, key);

            return oldValue;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Forces the association of the key with the value, removing any existing mappings
     * for either the key or the value.
     *
     * @param key The key to be associated with the value
     * @param value The value to be associated with the key
     * @return The previous value associated with the key, or null if there was no mapping
     */
    public V forcePut(K key, V value) {
        lock.writeLock().lock();
        try {
            // Remove existing mappings for the key and value
            V oldValue = forward.get(key);
            if (oldValue != null) {
                backward.remove(oldValue);
            }

            K existingKey = backward.get(value);
            if (existingKey != null) {
                forward.remove(existingKey);
            }

            // Add the new mappings
            forward.put(key, value);
            backward.put(value, key);

            return oldValue;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or null if this map contains no mapping for the key.
     *
     * @param key The key whose associated value is to be returned
     * @return The value to which the specified key is mapped, or
     *         null if this map contains no mapping for the key
     */
    public V get(K key) {
        lock.readLock().lock();
        try {
            return forward.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the key to which the specified value is mapped,
     * or null if this map contains no mapping for the value.
     *
     * @param value The value whose associated key is to be returned
     * @return The key to which the specified value is mapped, or
     *         null if this map contains no mapping for the value
     */
    public K getKey(V value) {
        lock.readLock().lock();
        try {
            return backward.get(value);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param key The key whose mapping is to be removed from the map
     * @return The previous value associated with the key, or null if there was no mapping
     */
    public V remove(K key) {
        lock.writeLock().lock();
        try {
            V value = forward.remove(key);
            if (value != null) {
                backward.remove(value);
            }
            return value;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes the mapping for the specified value from this map if present.
     *
     * @param value The value whose mapping is to be removed from the map
     * @return The previous key associated with the value, or null if there was no mapping
     */
    public K removeValue(V value) {
        lock.writeLock().lock();
        try {
            K key = backward.remove(value);
            if (key != null) {
                forward.remove(key);
            }
            return key;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns true if this map contains a mapping for the specified key.
     *
     * @param key The key whose presence in this map is to be tested
     * @return true if this map contains a mapping for the specified key
     */
    public boolean containsKey(K key) {
        lock.readLock().lock();
        try {
            return forward.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns true if this map maps one or more keys to the specified value.
     *
     * @param value The value whose presence in this map is to be tested
     * @return true if this map maps one or more keys to the specified value
     */
    public boolean containsValue(V value) {
        lock.readLock().lock();
        try {
            return backward.containsKey(value);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return The number of key-value mappings in this map
     */
    public int size() {
        lock.readLock().lock();
        try {
            return forward.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns true if this map contains no key-value mappings.
     *
     * @return true if this map contains no key-value mappings
     */
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return forward.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            forward.clear();
            backward.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns a Set view of the keys contained in this map.
     * The returned set is a snapshot of the keys at the time of call.
     *
     * @return A set view of the keys contained in this map
     */
    public Set<K> keySet() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableSet(forward.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns a Collection view of the values contained in this map.
     * The returned collection is a snapshot of the values at the time of call.
     *
     * @return A collection view of the values contained in this map
     */
    public Collection<V> values() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableCollection(forward.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns a Set view of the mappings contained in this map.
     * The returned set is a snapshot of the entries at the time of call.
     *
     * @return A set view of the mappings contained in this map
     */
    public Set<Map.Entry<K, V>> entrySet() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableSet(forward.entrySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns an inverse view of this BiMap, where the keys and values are swapped.
     *
     * @return A new ThreadSafeHashBiMap with keys and values swapped
     */
    public ThreadSafeHashBiMap<V, K> inverse() {
        lock.readLock().lock();
        try {
            ThreadSafeHashBiMap<V, K> inverse = new ThreadSafeHashBiMap<>();
            inverse.lock.writeLock().lock();
            try {
                inverse.forward.putAll(this.backward);
                inverse.backward.putAll(this.forward);
                return inverse;
            } finally {
                inverse.lock.writeLock().unlock();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Atomically adds all mappings from the specified map to this map.
     * If any value in the map is already present, an IllegalArgumentException is thrown.
     *
     * @param map The mappings to be stored in this map
     * @throws IllegalArgumentException if any value in the map is already mapped to a different key
     */
    public void putAll(Map<? extends K, ? extends V> map) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();

                // Check if the value is already mapped to a different key
                K existingKey = backward.get(value);
                if (existingKey != null && !existingKey.equals(key)) {
                    throw new IllegalArgumentException("Value " + value +
                            " already exists in the map with a different key: " + existingKey);
                }
            }

            // After validation, add all entries
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();

                // Remove previous mapping for this key if it exists
                V oldValue = forward.get(key);
                if (oldValue != null) {
                    backward.remove(oldValue);
                }

                // Add the new mappings
                forward.put(key, value);
                backward.put(value, key);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return forward.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}
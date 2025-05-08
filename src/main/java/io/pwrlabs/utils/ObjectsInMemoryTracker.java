package io.pwrlabs.utils;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ObjectsInMemoryTracker {
    // Map each tracked class → set of weak refs to its instances
    private static final ConcurrentMap<Class<?>, Set<WeakReference<Object>>> trackedObjects = new ConcurrentHashMap<>();

    /**
     * Call this in your tracked-class constructor:
     *     ObjectsInMemoryTracker.trackObject(this);
     */
    public static <T> void trackObject(T object) {
        Class<?> cls = object.getClass();
        cleanupStale(cls);
        trackedObjects
                .computeIfAbsent(cls, c -> ConcurrentHashMap.newKeySet())
                .add(new WeakReference<>(object));
    }

    /**
     * Returns how many live instances of classType are still in memory.
     * Automatically purges any that have been garbage-collected for that class.
     */
    public static int getObjectCountInMemory(Class<?> classType) {
        cleanupStale(classType);
        Set<WeakReference<Object>> refs = trackedObjects.get(classType);
        return (refs == null) ? 0 : refs.size();
    }

    /**
     * Remove any WeakReferences for the given class whose referent has been GC'd.
     */
    private static void cleanupStale(Class<?> classType) {
        Set<WeakReference<Object>> refs = trackedObjects.get(classType);
        if (refs != null) {
            refs.removeIf(ref -> ref.get() == null);
            // Optionally remove the entry entirely when empty:
            if (refs.isEmpty()) {
                trackedObjects.remove(classType, refs);
            }
        }
    }

    /**
     * Returns a map of each class you’ve tracked → the number of live instances still in memory.
     * Automatically cleans up any GC’d references before counting.
     */
    public static Map<Class<?>, Integer> getAllObjectCounts() {
        Map<Class<?>, Integer> counts = new HashMap<>();
        for (Class<?> cls : trackedObjects.keySet()) {
            // prune stale refs for this class
            cleanupStale(cls);
            Set<WeakReference<Object>> refs = trackedObjects.get(cls);
            counts.put(cls, refs != null ? refs.size() : 0);
        }
        return counts;
    }
}

//package io.pwrlabs.utils;
//
//import java.lang.ref.WeakReference;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//
//public class ObjectsInMemoryTracker {
//    /**
//     * Class to store both the object reference and the thread ID of its creation
//     */
//    private static class TrackedObject {
//        private final WeakReference<Object> reference;
//        private final long threadId;
//
//        public TrackedObject(Object object) {
//            this.reference = new WeakReference<>(object);
//            this.threadId = Thread.currentThread().getId();
//        }
//
//        public Object get() {
//            return reference.get();
//        }
//
//        public long getThreadId() {
//            return threadId;
//        }
//    }
//
//    // Map each tracked class → set of TrackedObject instances
//    private static final ConcurrentMap<Class<?>, Set<TrackedObject>> trackedObjects = new ConcurrentHashMap<>();
//
//    /**
//     * Call this in your tracked-class constructor:
//     *     //ObjectsInMemoryTracker.trackObject(this);
//     */
//    public static <T> void trackObject(T object) {
//        Class<?> cls = object.getClass();
//        cleanupStale(cls);
//        trackedObjects
//                .computeIfAbsent(cls, c -> ConcurrentHashMap.newKeySet())
//                .add(new TrackedObject(object));
//    }
//
//    /**
//     * Returns how many live instances of classType are still in memory.
//     * Automatically purges any that have been garbage-collected for that class.
//     */
//    public static int getObjectCountInMemory(Class<?> classType) {
//        cleanupStale(classType);
//        Set<TrackedObject> tracked = trackedObjects.get(classType);
//        return (tracked == null) ? 0 : tracked.size();
//    }
//
//    /**
//     * Returns how many live instances of classType created in the specified thread are still in memory.
//     * Automatically purges any that have been garbage-collected for that class.
//     */
//    public static int getObjectCountInMemoryByThread(Class<?> classType, long threadId) {
//        cleanupStale(classType);
//        Set<TrackedObject> tracked = trackedObjects.get(classType);
//        if (tracked == null) {
//            return 0;
//        }
//        return (int) tracked.stream()
//                .filter(to -> to.getThreadId() == threadId)
//                .count();
//    }
//
//    /**
//     * Remove any TrackedObjects for the given class whose referent has been GC'd.
//     */
//    private static void cleanupStale(Class<?> classType) {
//        Set<TrackedObject> tracked = trackedObjects.get(classType);
//        if (tracked != null) {
//            tracked.removeIf(to -> to.get() == null);
//            // Optionally remove the entry entirely when empty:
//            if (tracked.isEmpty()) {
//                trackedObjects.remove(classType, tracked);
//            }
//        }
//    }
//
//    /**
//     * Returns a map of each class you've tracked → the number of live instances still in memory.
//     * Automatically cleans up any GC'd references before counting.
//     */
//    public static Map<Class<?>, Integer> getAllObjectCounts() {
//        Map<Class<?>, Integer> counts = new HashMap<>();
//        for (Class<?> cls : trackedObjects.keySet()) {
//            // prune stale refs for this class
//            cleanupStale(cls);
//            Set<TrackedObject> tracked = trackedObjects.get(cls);
//            counts.put(cls, tracked != null ? tracked.size() : 0);
//        }
//        return counts;
//    }
//
//    /**
//     * Returns a map of each class you've tracked → a map of thread IDs → count of live instances
//     * for that thread.
//     * Automatically cleans up any GC'd references before counting.
//     */
//    public static Map<Class<?>, Map<Long, Integer>> getAllObjectCountsByThread() {
//        Map<Class<?>, Map<Long, Integer>> results = new HashMap<>();
//
//        for (Class<?> cls : trackedObjects.keySet()) {
//            // prune stale refs for this class
//            cleanupStale(cls);
//
//            Set<TrackedObject> tracked = trackedObjects.get(cls);
//            if (tracked != null && !tracked.isEmpty()) {
//                Map<Long, Integer> threadCounts = new HashMap<>();
//
//                // Count objects by thread ID
//                for (TrackedObject to : tracked) {
//                    long threadId = to.getThreadId();
//                    threadCounts.put(threadId, threadCounts.getOrDefault(threadId, 0) + 1);
//                }
//
//                results.put(cls, threadCounts);
//            }
//        }
//
//        return results;
//    }
//}
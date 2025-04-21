package io.pwrlabs.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static io.pwrlabs.newerror.NewError.errorIf;

public class PWRReentrantReadWriteLockManager {
    private static Map<String /*Lock Name*/, PWRReentrantReadWriteLock> readWriteLocks = new ConcurrentHashMap<>();

    public static synchronized PWRReentrantReadWriteLock createReadWriteLock(String name) {
        errorIf(readWriteLocks.containsKey(name), "Lock with name " + name + " already exists");

        PWRReentrantReadWriteLock lock = new PWRReentrantReadWriteLock();
        readWriteLocks.put(name, lock);

        return lock;
    }

    public Map<String, PWRReentrantReadWriteLock> getReadWriteLocksCopy() {
        return new ConcurrentHashMap<>(readWriteLocks);
    }
}

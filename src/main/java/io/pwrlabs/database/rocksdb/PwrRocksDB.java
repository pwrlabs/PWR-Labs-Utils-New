package io.pwrlabs.database.rocksdb;


import io.pwrlabs.concurrency.ConcurrentList;
import io.pwrlabs.util.files.FileUtils;
import io.pwrlabs.hashing.PWRHash;
import org.bouncycastle.util.encoders.Hex;
import org.rocksdb.*;

import java.awt.image.AreaAveragingScaleFilter;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.pwrlabs.newerror.NewError.errorIf;

public class PwrRocksDB {
    private final RocksDB db;
    private final Options options;
    private final String dbPath;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReadWriteLock constructorLock = new ReentrantReadWriteLock();

    private static final Map<String /*Path*/, PwrRocksDB> instances = new ConcurrentHashMap<>();
    private static final ConcurrentList<PwrRocksIterator> activeIterators = new ConcurrentList<>();

    protected PwrRocksDB(RocksDB db, Options options) {
        constructorLock.writeLock().lock();
        try {
            this.dbPath = db.getName();
            errorIf(instances.containsKey(dbPath), "Database already exists: " + dbPath);

            this.db = db;
            this.options = options;

            instances.put(dbPath, this);

            //Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        } finally {
            constructorLock.writeLock().unlock();
        }
    }

    static {
        RocksDB.loadLibrary();
    }

    //    // Create a new static factory method that properly handles the native handle
    public static synchronized PwrRocksDB open(Options options, String path) throws RocksDBException {
        PwrRocksDB PwrRocksDB = instances.get(path);
        if (PwrRocksDB != null) return PwrRocksDB;

        // Get the native handle from RocksDB.open()
        RocksDB db = RocksDB.open(options, path);

        // Create a new PwrRocksDB instance with this handle
        PwrRocksDB = new PwrRocksDB(db, options);
        return PwrRocksDB;
    }

    public static synchronized PwrRocksDB openWithDefaultOptions(String path) throws RocksDBException {
        File dbDir = new File(path);
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }

        Options options = new Options().setCreateIfMissing(true)
                .setMaxTotalWalSize(45 * 1024 * 1024L)  // Good: Limits total WAL to 512MB
                .setWalSizeLimitMB(15)                    // Good: Standard size per WAL file
                .setWalTtlSeconds(24 * 60 * 60)           // Good: Cleans up old WAL files after 24h
                .setInfoLogLevel(InfoLogLevel.FATAL_LEVEL)  // Good: Minimizes logging overhead
                .setDbLogDir("")                          // Good: Disables separate log directory
                .setLogFileTimeToRoll(0);                 // Good: Immediate roll when size limit reached

        // Add these additional safety options
        options.setAllowMmapReads(false)  // Disable memory mapping
                .setAllowMmapWrites(false)
                .setMaxOpenFiles(1000)
                .setMaxFileOpeningThreads(10)
                .setIncreaseParallelism(1); // Single-threaded mode is safer

        options.setParanoidChecks(true)  // Enable paranoid checks for corruption
                .setUseDirectReads(true)  // Direct I/O for reads
                .setUseDirectIoForFlushAndCompaction(true)  // Direct I/O for writes
                .setEnableWriteThreadAdaptiveYield(true)
                .setAllowConcurrentMemtableWrite(true);

        return open(options, path);
    }

    public static synchronized PwrRocksDB getExistingDB(String path) {
        return instances.get(path);
    }

    public void put(byte[] key, byte[] value) throws RocksDBException {
        lock.writeLock().lock();
        try {
            byte[] protectedData = addCorruptionGuard(value);
            db.put(key, protectedData);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public byte[] get(byte[] key) throws RocksDBException {
        lock.readLock().lock();
        try {
            byte[] value = db.get(key);
            if (value == null) return null;

            if (!verifyCorruptionGuard(value)) {
                System.err.println("Corruption guard failed for key: " + new String(key) + ", corrupted data detected");
                System.err.println("Data size: " + value.length);
                System.err.println("Data: " + Hex.toHexString(value));
                System.err.println("Shutting application");
                System.exit(0);
            }

            return removeCorruptionGuard(value);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void delete(byte[] key) throws RocksDBException {
        lock.writeLock().lock();
        try {
            db.delete(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getLatestSequenceNumber() {
        lock.readLock().lock();
        try {
            return db.getLatestSequenceNumber();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Creates a new RocksIterator. The caller is responsible for closing the iterator.
     * Usage:
     * try (RocksIterator iterator = db.newIterator()) {
     *     // use iterator
     * }
     */
    public PwrRocksIterator newIterator() {
        //if (closing) return null;
        lock.readLock().lock();
        try {
            clearClosedIterators();

            RocksIterator iterator = db.newIterator();
            PwrRocksIterator pwrRocksIterator = new PwrRocksIterator(iterator);
            activeIterators.add(pwrRocksIterator);

            return pwrRocksIterator;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<byte[]> getAllKeys() {
        //if (closing) return null;
        lock.readLock().lock();
        try {
            RocksIterator iterator = db.newIterator();
            List<byte[]> keys = new java.util.ArrayList<>();
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                keys.add(iterator.key());
            }
            return keys;
        } finally {
            lock.readLock().unlock();
        }
    }

    public static void clone(PwrRocksDB src, PwrRocksDB dst) throws RocksDBException {
        if(src == dst) return;
        if(src == null || dst == null) return;
        if(src.dbPath.equals(dst.dbPath)) return;

        src.lock.readLock().lock();
        dst.lock.writeLock().lock();
        try {
            File origDir = new File(src.dbPath);
            File destDir = new File(dst.dbPath);
            try (Checkpoint checkpoint = Checkpoint.create(src.db)) {
                dst.close();
                if (destDir.exists()) {
                    FileUtils.deleteDirectory(destDir);
                } else {
                    destDir.mkdirs();
                    FileUtils.deleteDirectory(destDir);
                }

                if (origDir.exists()) {
                    // Create a checkpoint and copy the state to the destination directory
                    checkpoint.createCheckpoint(destDir.getAbsolutePath());
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        } finally {
            src.lock.readLock().unlock();
            dst.lock.writeLock().unlock();
        }
    }

    public static void update(PwrRocksDB src, PwrRocksDB dst, List<byte[]> affectedKeys) throws RocksDBException {
        if(src == dst) return;
        if(src == null || dst == null) return;
        if(src.dbPath.equals(dst.dbPath)) return;

        src.lock.readLock().lock();
        dst.lock.writeLock().lock();
        try {
            try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions()) {
                for (byte[] key : affectedKeys) {
                    byte[] value = src.get(key);
                    if (value != null) {
                        batch.put(key, addCorruptionGuard(value));
                    } else {
                        batch.delete(key);
                    }
                }

                dst.db.write(writeOptions, batch);

            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }

            //compact the database
            src.db.compactRange();
            dst.db.compactRange();
        } finally {
            src.lock.readLock().unlock();
            dst.lock.writeLock().unlock();
        }
    }

    public void close() {
        //closing = true;
        lock.writeLock().lock();
        try {
            if(db != null && !db.isClosed()) {
                try { db.close(); } catch (Exception e) {e.printStackTrace();}
            }

            if(options != null) {
                try { options.close(); } catch (Exception e) { e.printStackTrace(); }
            }

            instances.remove(dbPath);

            while (!activeIterators.isEmpty()) {
                clearClosedIterators();
                Thread.yield();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isClosed() {
        return db.isClosed();
    }

    private void clearClosedIterators() {
        if(activeIterators == null || activeIterators.isEmpty()) return;

        for (PwrRocksIterator iterator : activeIterators.getArrayListCopy()) {
            if (iterator.isClosed()) {
                activeIterators.remove(iterator);
            }
        }
    }

    private static byte[] addCorruptionGuard(byte[] data) {
        if (data == null) return null;

        byte[] hash = PWRHash.hash224(data);

        ByteBuffer buffer = ByteBuffer.allocate(data.length + hash.length);
        buffer.put(data);
        buffer.put(hash);
        return buffer.array();
    }

    private static byte[] removeCorruptionGuard(byte[] data) {
        if (data == null || data.length < 28) {
            return data; // Return original data or handle error appropriately
        }

        byte[] result = new byte[data.length - 28];
        System.arraycopy(data, 0, result, 0, data.length - 28);
        return result;
    }

    public static boolean  verifyCorruptionGuard(byte[] data) {
        if(data.length < 29) return false;

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] dataWithoutHash = new byte[data.length - 28];
        byte[] hash = new byte[28];

        buffer.get(dataWithoutHash);
        buffer.get(hash);

        byte[] computedHash = PWRHash.hash224(dataWithoutHash);

        return Arrays.equals(hash, computedHash);
    }

}


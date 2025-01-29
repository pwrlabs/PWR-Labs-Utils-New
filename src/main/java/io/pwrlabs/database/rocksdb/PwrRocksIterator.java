package io.pwrlabs.database.rocksdb;

import org.rocksdb.RocksIterator;

public class PwrRocksIterator implements AutoCloseable {
    private final RocksIterator iterator;
    private boolean closed = false;

    public PwrRocksIterator(RocksIterator iterator) {
        this.iterator = iterator;
    }

    public void seekToFirst() {
        iterator.seekToFirst();
    }

    public boolean isValid() {
        return iterator.isValid();
    }

    public void next() {
        iterator.next();
    }

    public boolean isClosed() {
        return closed;
    }

    public byte[] key() {
        return iterator.key();
    }

    public byte[] value() {
        return iterator.value();
    }

    @Override
    public void close() {
        if (!closed) {
            iterator.close();
            closed = true;
        }
    }

    // Delegate other methods to the underlying iterator as needed...
}

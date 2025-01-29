package io.pwrlabs.concurrency;


import lombok.Getter;

import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentList<E> implements AutoCloseable {
    @Getter
    private static final AtomicInteger listCount = new AtomicInteger(0);
    private static final Cleaner CLEANER = Cleaner.create();
    private final Cleaner.Cleanable cleanable;

    private final List<E> list;
    private final ReadWriteLock lock;

    public ConcurrentList() {
        this.list = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();

        cleanable = CLEANER.register(this, new CleaningAction());
        listCount.incrementAndGet();
    }

    public ConcurrentList(int listSize) {
        this.list = new ArrayList<>(listSize);
        this.lock = new ReentrantReadWriteLock();

        cleanable = CLEANER.register(this, new CleaningAction());
        listCount.incrementAndGet();
    }

    public ConcurrentList(ConcurrentList<E> ConcurrentList) {
        this.list = new ArrayList<>(ConcurrentList.getList());
        this.lock = new ReentrantReadWriteLock();

        cleanable = CLEANER.register(this, new CleaningAction());
        listCount.incrementAndGet();
    }

    public ConcurrentList(List<E> list) {
        this.list = new ArrayList<>(list);
        this.lock = new ReentrantReadWriteLock();

        cleanable = CLEANER.register(this, new CleaningAction());
        listCount.incrementAndGet();
    }


    public void add(E element) {
        lock.writeLock().lock();
        try {
            list.add(element);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void add(int position, E element) {
        lock.writeLock().lock();
        try {
            list.add(position, element);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(E element) {
        lock.writeLock().lock();
        try {
            list.remove(element);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeAll(List<E> elements) {
        lock.writeLock().lock();
        try {
            list.removeAll(elements);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean contains(E element) {
        lock.readLock().lock();
        try {
            return list.contains(element);
        } finally {
            lock.readLock().unlock();
        }
    }

    public E get(int index) {
        lock.readLock().lock();
        try {
            return list.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<E> getList() {
        lock.readLock().lock();
        try {
            return list;
        } finally {
            lock.readLock().unlock();
        }
    }

    public ArrayList<E> getArrayListCopy() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(list);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return list.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            list.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return list.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    private static class CleaningAction implements Runnable {
        @Override
        public void run() {
            listCount.decrementAndGet();
        }
    }
}

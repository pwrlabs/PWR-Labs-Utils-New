package io.pwrlabs.database.rocksdb;

import com.sun.tools.javac.Main;
import io.pwrlabs.hashing.PWRHash;
import io.pwrlabs.util.encoders.BiResult;
import io.pwrlabs.util.encoders.ByteArrayWrapper;
import io.pwrlabs.util.encoders.Hex;
import io.pwrlabs.util.files.FileUtils;
import io.pwrlabs.utils.TriggerEvent;
import lombok.Getter;
import org.json.JSONObject;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.pwrlabs.newerror.NewError.errorIf;

/**
 * EdyMerkleTree: A Merkle Tree backed by RocksDB storage.
 */
public class MerkleTree {

    //region ===================== Statics =====================
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Main.class);

    static {
        RocksDB.loadLibrary();
    }

    private static Map<String /*Tree Name*/, MerkleTree> openTrees = new ConcurrentHashMap<>();

    @Getter
    private static AtomicInteger treesCloned = new AtomicInteger(0);
    @Getter
    private static AtomicInteger treesUpdatedWithoutClone = new AtomicInteger(0);

    public static List<MerkleTree> getAllOpenMerkleTrees() {
        List<MerkleTree> trees = new ArrayList<>();
        for (Map.Entry<String, MerkleTree> entry : openTrees.entrySet()) {
            trees.add(entry.getValue());
        }
        return trees;
    }
    //endregion

    //region ===================== Constants =====================
    private static final int HASH_LENGTH = 32;
    private static final String METADATA_DB_NAME = "metaData";
    private static final String NODES_DB_NAME = "nodes";
    private static final String KEY_DATA_DB_NAME = "keyData";

    // Metadata Keys
    private static final String KEY_ROOT_HASH = "rootHash";
    private static final String KEY_NUM_LEAVES = "numLeaves";
    private static final String KEY_DEPTH = "depth";
    private static final String KEY_HANGING_NODE_PREFIX = "hangingNode";
    //endregion

    //region ===================== Fields =====================
    @Getter
    private final String treeName;
    @Getter
    private final String path;

    private RocksDB db;
    private ColumnFamilyHandle metaDataHandle;
    private ColumnFamilyHandle nodesHandle;
    private ColumnFamilyHandle keyDataHandle;


    /**
     * Cache of loaded nodes (in-memory for quick access).
     */
    private final Map<ByteArrayWrapper, Node> nodesCache = new ConcurrentHashMap<>();
    private final Map<Integer /*level*/, byte[]> hangingNodes = new ConcurrentHashMap<>();
    /**
     * Stores key–value entries that have already been incorporated into the Merkle tree
     * and have contributed to the current root hash.
     * This cache is used to quickly retrieve recently committed values without having to
     * read them from RocksDB or re-traverse the Merkle tree.
     */
    private final Map<ByteArrayWrapper /*Key*/, byte[] /*data*/> keyDataCommittedCache = new ConcurrentHashMap<>();
    /**
     * Stores key–value entries that have been written or updated but have not yet been
     * incorporated into the Merkle tree, and therefore have not affected the current root hash.
     * This acts as a staging area for pending state updates before they are committed.
     */
    private final Map<ByteArrayWrapper /*Key*/, byte[] /*data*/> keyDataPendingCache = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<PendingChanges> pendingChangesQueue = new LinkedBlockingQueue<>();


    @Getter
    private int numLeaves = 0;
    @Getter
    private int depth = 0;
    private byte[] rootHash = null;

    private AtomicBoolean closed = new AtomicBoolean(false);
    private AtomicBoolean hasUnsavedChanges = new AtomicBoolean(false);
    private AtomicBoolean trackTimeOfOperations = new AtomicBoolean(false);
    private AtomicBoolean pendingChangesBeingProcessed = new AtomicBoolean(false);

    //A lock to prevent functions that modify the tree from being called concurrently- such as update, clone, addOrUpdateData, etc.
    private final ReentrantLock writeLock = new ReentrantLock();
    private final TriggerEvent pendingChangesProcessedEvent = new TriggerEvent();
    //endregion

    //region ===================== Constructors =====================
    public MerkleTree(String treeName) throws RocksDBException {
        //ObjectsInMemoryTracker.trackObject(this);
        RocksDB.loadLibrary();
        this.treeName = treeName;
        errorIf(openTrees.containsKey(treeName), "There is already open instance of this tree");

        // 1. Ensure directory exists
        path = "merkleTree/" + treeName;
        File directory = new File(path);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new RocksDBException("Failed to create directory: " + path);
        }

        // 2. Initialize DB
        initializeDb();

        // 7. Load initial metadata
        loadMetaData();

        // 8. Register instance
        openTrees.put(treeName, this);

        // 9. Force manual compaction on startup to reduce memory footprint
        try {
            db.compactRange();
        } catch (Exception e) {
            // Ignore compaction errors
        }

        initPendingChangesProcessor();
    }

    public MerkleTree(String treeName, boolean trackTimeOfOperations) throws RocksDBException {
        this(treeName);
        this.trackTimeOfOperations.set(trackTimeOfOperations);
    }
    //endregion

    //region ===================== Public Methods =====================

    /**
     * Get the current root hash of the Merkle tree.
     */
    public byte[] getRootHash() {
        errorIfClosed();
        try {
            if (keyDataPendingCache.size() > 0) {
                System.out.println(treeName + " has pending changes, waiting for them to be processed before returning root hash");
                pendingChangesProcessedEvent.awaitEvent();
            }
            if (rootHash == null) return null;
            else return Arrays.copyOf(rootHash, rootHash.length);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] getRootHashSavedOnDisk() {
        errorIfClosed();
        try {
            return db.get(metaDataHandle, KEY_ROOT_HASH.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public int getNumLeaves() {
        errorIfClosed();

        return numLeaves;

    }

    public int getDepth() {
        errorIfClosed();

        return depth;
    }

    /**
     * Get all nodes saved on disk.
     *
     * @return A list of all nodes in the tree
     * @throws RocksDBException If there's an error accessing RocksDB
     */
    public HashSet<Node> getAllNodes() throws RocksDBException {
        errorIfClosed();

        HashSet<Node> allNodes = new HashSet<>();

        // First flush any pending changes to disk
        flushToDisk();

        // Use RocksIterator to iterate through all nodes in the nodesHandle column family
        try (RocksIterator iterator = db.newIterator(nodesHandle)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                byte[] nodeData = iterator.value();

                // Decode the node from its binary representation
                Node node = decodeNode(nodeData);
                allNodes.add(node);

                iterator.next();
            }
        }

        return allNodes;
    }

    /**
     * Get data for a key from the Merkle Tree.
     *
     * @param key The key to retrieve data for
     * @return The data for the key, or null if the key doesn't exist
     * @throws RocksDBException         If there's an error accessing RocksDB
     * @throws IllegalArgumentException If key is null
     */
    public byte[] getData(byte[] key) {
        errorIfClosed();
        byte[] data = keyDataPendingCache.get(new ByteArrayWrapper(key));
        if (data == null) keyDataCommittedCache.get(new ByteArrayWrapper(key));
        if (data != null) return data;

        try {
            return db.get(keyDataHandle, key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] getCommittedData(byte[] key) {
        errorIfClosed();
        byte[] data = keyDataCommittedCache.get(new ByteArrayWrapper(key));
        if (data != null) return data;

        try {
            return db.get(keyDataHandle, key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add or update data for a key in the Merkle Tree.
     * This will create a new leaf node with a hash derived from the key and data,
     * or update an existing leaf if the key already exists.
     *
     * @param key  The key to store data for
     * @param data The data to store
     * @throws RocksDBException         If there's an error accessing RocksDB
     * @throws IllegalArgumentException If key or data is null
     */
    public void addOrUpdateData(byte[] key, byte[] data) throws RocksDBException {
        errorIfClosed();

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }

        writeLock.lock();
        try {
            pendingChangesQueue.add(new PendingChanges(key, data));
            keyDataPendingCache.put(new ByteArrayWrapper(key), data);
            hasUnsavedChanges.set(true);
        } finally {
            writeLock.unlock();
        }
    }

    public void revertUnsavedChanges() {
        if (!hasUnsavedChanges.get()) return;
        errorIfClosed();

        writeLock.lock();
        try {
            nodesCache.clear();
            hangingNodes.clear();
            keyDataCommittedCache.clear();
            keyDataPendingCache.clear();
            pendingChangesQueue.clear();

            if(pendingChangesBeingProcessed.get()) {
                // Wait for pending changes to be processed before clearing
                try {
                    pendingChangesProcessedEvent.awaitEvent();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for pending changes to be processed", e);
                }
            }

            loadMetaData();

            hasUnsavedChanges.set(false);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            writeLock.unlock();
        }
    }

    public boolean containsKey(byte[] key) {
        errorIfClosed();

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        try {
            return db.get(keyDataHandle, key) != null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public List<byte[]> getAllKeys() {
        errorIfClosed();

        List<byte[]> keys = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(keyDataHandle)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                keys.add(iterator.key());
                iterator.next();
            }
        }
        return keys;
    }

    public List<byte[]> getAllData() {
        errorIfClosed();

        List<byte[]> data = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(keyDataHandle)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                data.add(iterator.value());
                iterator.next();
            }
        }
        return data;
    }

    public BiResult<List<byte[]>, List<byte[]>> keysAndTheirValues() {
        errorIfClosed();
        List<byte[]> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(keyDataHandle)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                keys.add(iterator.key());
                values.add(iterator.value());
                iterator.next();
            }
        }
        return new BiResult<>(keys, values);
    }

    /**
     * Flush all in-memory changes (nodes, metadata) to RocksDB.
     */
    public void flushToDisk() throws RocksDBException {
        if (!hasUnsavedChanges.get()) return;
        errorIfClosed();

        writeLock.lock();
        if( pendingChangesBeingProcessed.get()) {
            // Wait for pending changes to be processed before flushing
            try {
                pendingChangesProcessedEvent.awaitEvent();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for pending changes to be processed", e);
            }
        }
        try (WriteBatch batch = new WriteBatch()) {
            //Clear old metadata from disk
            try (RocksIterator iterator = db.newIterator(metaDataHandle)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    batch.delete(metaDataHandle, iterator.key());
                    iterator.next();
                }
            }

            if (rootHash != null) {
                batch.put(metaDataHandle, KEY_ROOT_HASH.getBytes(), rootHash);
            } else {
                batch.delete(metaDataHandle, KEY_ROOT_HASH.getBytes());
            }
            batch.put(metaDataHandle, KEY_NUM_LEAVES.getBytes(), ByteBuffer.allocate(4).putInt(numLeaves).array());
            batch.put(metaDataHandle, KEY_DEPTH.getBytes(), ByteBuffer.allocate(4).putInt(depth).array());

            for (Map.Entry<Integer, byte[]> entry : hangingNodes.entrySet()) {
                Integer level = entry.getKey();
                byte[] nodeHash = entry.getValue();
                batch.put(metaDataHandle, (KEY_HANGING_NODE_PREFIX + level).getBytes(), nodeHash);
            }

            for (Node node : nodesCache.values()) {
                batch.put(nodesHandle, node.hash, node.encode());

                if (node.getNodeHashToRemoveFromDb() != null) {
                    batch.delete(nodesHandle, node.getNodeHashToRemoveFromDb());
                }
            }

            for (Map.Entry<ByteArrayWrapper, byte[]> entry : keyDataCommittedCache.entrySet()) {
                batch.put(keyDataHandle, entry.getKey().data(), entry.getValue());
            }

            try (WriteOptions writeOptions = new WriteOptions()) {
                db.write(writeOptions, batch);
            }

            nodesCache.clear();
            keyDataCommittedCache.clear();
            hasUnsavedChanges.set(false);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Close the databases (optional, if you need cleanup).
     */
    public void close() throws RocksDBException {
        if (closed.get()) return;
        writeLock.lock();
        try {
            flushToDisk();

            if (metaDataHandle != null) {
                try {
                    metaDataHandle.close();
                } catch (Exception e) {
                    // Log error
                }
            }

            if (nodesHandle != null) {
                try {
                    nodesHandle.close();
                } catch (Exception e) {
                    // Log error
                }
            }

            if (keyDataHandle != null) {
                try {
                    keyDataHandle.close();
                } catch (Exception e) {
                    // Log error
                }
            }

            if (db != null) {
                try {
                    db.close();
                } catch (Exception e) {
                    // Log error
                }
            }

            openTrees.remove(treeName);
            closed.set(true);
        } finally {
            writeLock.unlock();
        }
    }

    public MerkleTree clone(String newTreeName) throws RocksDBException, IOException {
        logger.info("Cloning MerkleTree: " + treeName + " to new tree: " + newTreeName);
        errorIfClosed();

        writeLock.lock();
        try {
            if (newTreeName == null || newTreeName.isEmpty()) {
                throw new IllegalArgumentException("New tree name cannot be null or empty");
            }

            if (openTrees.containsKey(newTreeName)) {
                MerkleTree existingTree = openTrees.get(newTreeName);
                existingTree.close();
            }

            File destDir = new File("merkleTree/" + newTreeName);

            if (destDir.exists()) {
                FileUtils.deleteDirectory(destDir);
            } else {
                // If the directory has sub-directories then create them without creating the directory itself
                File parent = destDir.getParentFile();
                if (parent != null && !parent.exists()) {
                    if (!parent.mkdirs()) {
                        throw new IOException("Failed to create parent directories for " + destDir);
                    }
                }
            }

            flushToDisk();

            try (Checkpoint checkpoint = Checkpoint.create(db)) {
                checkpoint.createCheckpoint(destDir.getAbsolutePath());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            return new MerkleTree(newTreeName);
        } finally {
            writeLock.unlock();
        }
    }

    public void update(MerkleTree sourceTree) throws RocksDBException, IOException {
        errorIfClosed();

        if (sourceTree == null) {
            throw new IllegalArgumentException("Source tree cannot be null");
        }

        writeLock.lock();
        sourceTree.writeLock.lock();
        try {
            byte[] sourceRootHash = sourceTree.getRootHash();
            byte[] thisRootHash = getRootHash();
            if (sourceRootHash == null) {
                if (thisRootHash == null) return;
                else clear();
            } else {
                byte[] rootHashSavedOnDisk = getRootHashSavedOnDisk();
                byte[] sourceRootHashSavedOnDisk = sourceTree.getRootHashSavedOnDisk();

                if (
                        (rootHashSavedOnDisk == null && sourceRootHashSavedOnDisk == null)
                                ||
                                (rootHashSavedOnDisk != null && sourceRootHashSavedOnDisk != null)
                                        && Arrays.equals(getRootHashSavedOnDisk(), sourceTree.getRootHashSavedOnDisk())
                ) {
                    //This means that this tree is already a copy of the source tree and we only need to replace the cache
                    logger.info("Updating MerkleTree: " + treeName + " with source tree: " + sourceTree.treeName + " by updating the cache");
                    copyCache(sourceTree);
                    treesUpdatedWithoutClone.incrementAndGet();
                } else {
                    logger.info("Updating MerkleTree: " + treeName + " with source tree: " + sourceTree.treeName + " by cloning the tree");
                    if (metaDataHandle != null) {
                        metaDataHandle.close();
                        metaDataHandle = null;
                    }
                    if (nodesHandle != null) {
                        nodesHandle.close();
                        nodesHandle = null;
                    }
                    if (keyDataHandle != null) {
                        keyDataHandle.close();
                        keyDataHandle = null;
                    }
                    if (db != null && !db.isClosed()) {
                        db.close();
                        db = null;
                    }
                    ;

                    sourceTree.flushToDisk();

                    File thisTreesDirectory = new File(path);
                    FileUtils.deleteDirectory(thisTreesDirectory);

                    try (Checkpoint checkpoint = Checkpoint.create(sourceTree.db)) {
                        checkpoint.createCheckpoint(thisTreesDirectory.getAbsolutePath());
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }

                    // Reinitialize the database
                    initializeDb();
                    loadMetaData();

                    nodesCache.clear();
                    keyDataCommittedCache.clear();
                    hangingNodes.clear();
                    hasUnsavedChanges.set(false);
                    treesCloned.incrementAndGet();
                }
            }
        } finally {
            sourceTree.writeLock.unlock();
            writeLock.unlock();
        }
    }

    /**
     * Efficiently clears the entire MerkleTree by closing, deleting and recreating the RocksDB instance.
     * This is much faster than iterating through all entries and deleting them individually.
     */
    public void clear() throws RocksDBException {
        writeLock.lock();
        try {
        byte[] start = new byte[0];
        byte[] end = new byte[]{(byte) 0xFF};

        // these three calls are each O(1)
        db.deleteRange(metaDataHandle, start, end);
        db.deleteRange(nodesHandle, start, end);
        db.deleteRange(keyDataHandle, start, end);

        // OPTIONAL: reclaim space immediately
        db.compactRange(metaDataHandle);
        db.compactRange(nodesHandle);
        db.compactRange(keyDataHandle);

        // reset your in-memory state
        nodesCache.clear();
        keyDataCommittedCache.clear();
        keyDataPendingCache.clear();
        hangingNodes.clear();
        rootHash = null;
        numLeaves = depth = 0;
        hasUnsavedChanges.set(false);
        pendingChangesQueue.clear();
        } finally {
            writeLock.unlock();
        }
    }

    public JSONObject getRamInfo() {
        JSONObject json = new JSONObject();
        json.put("treeName", treeName);
        json.put("numLeaves", numLeaves);
        json.put("depth", depth);
        json.put("nodeCacheSize", nodesCache.size());
        json.put("keyDataCacheSize", keyDataCommittedCache.size());
        json.put("hangingNodesCacheSize", hangingNodes.size());
        return json;
    }

    //endregion

    //region ===================== Private Methods =====================
    private void initializeDb() throws RocksDBException {
        // 1) DBOptions
        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxOpenFiles(1000)
                .setMaxBackgroundJobs(1)
                .setInfoLogLevel(InfoLogLevel.FATAL_LEVEL)
                .setAllowMmapReads(true)
                .setAllowMmapWrites(false);
        ;  // Enable memory-mapped reads for better performance
        // (omit setNoBlockCache or any “disable cache” flags)

        // 2) Table format: enable a 64 MB off-heap LRU cache
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setBlockCache(new LRUCache(16 * 1024L * 1024L))  // 64 MiB
                .setFilterPolicy(new BloomFilter(10, false))
                .setBlockSize(32 * 1024)        // 16 KiB blocks
                .setFormatVersion(5)
                .setChecksumType(ChecksumType.kxxHash)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true);

        // 3) ColumnFamilyOptions: reference our tableConfig
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                .setTableFormatConfig(tableConfig)
                .setCompressionType(CompressionType.NO_COMPRESSION)  // No compression for max read speed
                .setBottommostCompressionType(CompressionType.NO_COMPRESSION)
                .setWriteBufferSize(8 * 1024 * 1024)
                .setMaxWriteBufferNumber(1)
                .setMinWriteBufferNumberToMerge(1)
                .optimizeUniversalStyleCompaction()
                .optimizeForPointLookup(1_000_000);


        // 4) Prepare column-family descriptors & handles as before…
        List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                new ColumnFamilyDescriptor(METADATA_DB_NAME.getBytes(), cfOptions),
                new ColumnFamilyDescriptor(NODES_DB_NAME.getBytes(), cfOptions),
                new ColumnFamilyDescriptor(KEY_DATA_DB_NAME.getBytes(), cfOptions)
        );
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        // 5) Open the DB
        this.db = RocksDB.open(dbOptions, path, cfDescriptors, cfHandles);

        // 6) Assign handles…
        this.metaDataHandle = cfHandles.get(1);
        this.nodesHandle = cfHandles.get(2);
        this.keyDataHandle = cfHandles.get(3);
    }

    /**
     * Load the tree's metadata from RocksDB.
     */
    private void loadMetaData() throws RocksDBException {
        this.rootHash = db.get(metaDataHandle, KEY_ROOT_HASH.getBytes());

        byte[] numLeavesBytes = db.get(metaDataHandle, KEY_NUM_LEAVES.getBytes());
        this.numLeaves = (numLeavesBytes == null)
                ? 0
                : ByteBuffer.wrap(numLeavesBytes).getInt();

        byte[] depthBytes = db.get(metaDataHandle, KEY_DEPTH.getBytes());
        this.depth = (depthBytes == null)
                ? 0
                : ByteBuffer.wrap(depthBytes).getInt();

        // Load any hangingNodes from metadata
        hangingNodes.clear();
        for (int i = 0; i <= depth; i++) {
            byte[] hash = db.get(metaDataHandle, (KEY_HANGING_NODE_PREFIX + i).getBytes());
            if (hash != null) {
                Node node = getNodeByHash(hash);
                hangingNodes.put(i, node.hash);
            }
        }
    }

    /**
     * Fetch a node by its hash, either from the in-memory cache or from RocksDB.
     */
    private Node getNodeByHash(byte[] hash) throws RocksDBException {
        if (hash == null) return null;

        ByteArrayWrapper baw = new ByteArrayWrapper(hash);
        Node node = nodesCache.get(baw);
        if (node == null) {
            try {
                byte[] encodedData = db.get(nodesHandle, hash);
                if (encodedData == null) {
                    return null;
                }
                node = decodeNode(encodedData);
                nodesCache.put(baw, node);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        return node;
    }

    /**
     * Decode a node from bytes.
     */
    private Node decodeNode(byte[] encodedData) {
        ByteBuffer buffer = ByteBuffer.wrap(encodedData);

        byte[] hash = new byte[HASH_LENGTH];
        buffer.get(hash);

        boolean hasLeft = buffer.get() == 1;
        boolean hasRight = buffer.get() == 1;
        boolean hasParent = buffer.get() == 1;

        byte[] left = hasLeft ? new byte[HASH_LENGTH] : null;
        byte[] right = hasRight ? new byte[HASH_LENGTH] : null;
        byte[] parent = hasParent ? new byte[HASH_LENGTH] : null;

        if (hasLeft) buffer.get(left);
        if (hasRight) buffer.get(right);
        if (hasParent) buffer.get(parent);

        return new Node(hash, left, right, parent);
    }

    private byte[] calculateLeafHash(byte[] key, byte[] data) {
        return PWRHash.hash256(key, data);
    }

    /**
     * Add a new leaf node to the Merkle Tree.
     */
    private void addLeaf(Node leafNode) throws RocksDBException {
        if (leafNode == null) {
            throw new IllegalArgumentException("Leaf node cannot be null");
        }
        if (leafNode.hash == null) {
            throw new IllegalArgumentException("Leaf node hash cannot be null");
        }

        if (numLeaves == 0) {
            hangingNodes.put(0, leafNode.hash);
            rootHash = leafNode.hash;
        } else {
            Node hangingLeaf = getNodeByHash(hangingNodes.get(0));

            // If there's no hanging leaf at level 0, place this one there.
            if (hangingLeaf == null) {
                hangingNodes.put(0, leafNode.hash);

                Node parentNode = new Node(leafNode.hash, null);
                leafNode.setParentNodeHash(parentNode.hash);
                addNode(1, parentNode);
            } else {
                // If a leaf is already hanging, connect this leaf with the parent's parent
                if (hangingLeaf.parent == null) { //If the hanging leaf is the root
                    Node parentNode = new Node(hangingLeaf.hash, leafNode.hash);
                    hangingLeaf.setParentNodeHash(parentNode.hash);
                    leafNode.setParentNodeHash(parentNode.hash);
                    addNode(1, parentNode);
                } else {
                    Node parentNodeOfHangingLeaf = getNodeByHash(hangingLeaf.parent);
                    if (parentNodeOfHangingLeaf == null) {
                        throw new IllegalStateException("Parent node of hanging leaf not found");
                    }
                    parentNodeOfHangingLeaf.addLeaf(leafNode.hash);
                }
                hangingNodes.remove(0);
            }
        }

        numLeaves++;
    }

    private void updateLeaf(byte[] oldLeafHash, byte[] newLeafHash) {
        if (oldLeafHash == null) {
            throw new IllegalArgumentException("Old leaf hash cannot be null");
        }
        if (newLeafHash == null) {
            throw new IllegalArgumentException("New leaf hash cannot be null");
        }
        errorIf(Arrays.equals(oldLeafHash, newLeafHash), "Old and new leaf hashes cannot be the same");

        //getWriteLock();
        try {
            Node leaf = getNodeByHash(oldLeafHash);

            if (leaf == null) {
                throw new IllegalArgumentException("Leaf not found: " + Hex.toHexString(oldLeafHash));
            } else {
                leaf.updateNodeHash(newLeafHash);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Error updating leaf: " + e.getMessage(), e);
        } finally {
            // releaseWriteLock();
        }
    }

    /**
     * Add a node at a given level.
     */
    private void addNode(int level, Node node) throws RocksDBException {
        if (level > depth) depth = level;
        Node hangingNode = getNodeByHash(hangingNodes.get(level));

        if (hangingNode == null) {
            // No hanging node at this level, so let's hang this node.
            hangingNodes.put(level, node.hash);

            // If this level is the depth level, this node's hash is the new root hash
            if (level >= depth) {
                rootHash = node.hash;
            } else {
                // Otherwise, create a parent and keep going up
                Node parentNode = new Node(node.hash, null);
                node.setParentNodeHash(parentNode.hash);
                addNode(level + 1, parentNode);
            }
        } else if (hangingNode.parent == null) {
            Node parent = new Node(hangingNode.hash, node.hash);
            hangingNode.setParentNodeHash(parent.hash);
            node.setParentNodeHash(parent.hash);
            hangingNodes.remove(level);
            addNode(level + 1, parent);
        } else {
            // If a node is already hanging at this level, attach the new node as a leaf
            Node parentNodeOfHangingNode = getNodeByHash(hangingNode.parent);
            if (parentNodeOfHangingNode != null) {
                parentNodeOfHangingNode.addLeaf(node.hash);
                hangingNodes.remove(level);
            } else {
                // If parent node is null, create a new parent node
                Node parent = new Node(hangingNode.hash, node.hash);
                hangingNode.setParentNodeHash(parent.hash);
                node.setParentNodeHash(parent.hash);
                hangingNodes.remove(level);
                addNode(level + 1, parent);
            }
        }
    }

    private void errorIfClosed() {
        if (closed.get()) {
            throw new IllegalStateException("MerkleTree is closed");
        }
    }

    private void copyCache(MerkleTree sourceTree) {
        nodesCache.clear();
        keyDataCommittedCache.clear();
        hangingNodes.clear();

        for (Map.Entry<ByteArrayWrapper, Node> entry : sourceTree.nodesCache.entrySet()) {
            nodesCache.put(entry.getKey(), new Node(entry.getValue()));
        }

        for (Map.Entry<ByteArrayWrapper, byte[]> entry : sourceTree.keyDataCommittedCache.entrySet()) {
            keyDataCommittedCache.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }

        for (Map.Entry<Integer, byte[]> entry : sourceTree.hangingNodes.entrySet()) {
            hangingNodes.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }

        rootHash = sourceTree.rootHash == null ? null : Arrays.copyOf(sourceTree.rootHash, sourceTree.rootHash.length);
        numLeaves = sourceTree.numLeaves;
        depth = sourceTree.depth;
        hasUnsavedChanges.set(sourceTree.hasUnsavedChanges.get());
    }

    private void initPendingChangesProcessor() {
        Thread thread = new Thread(() -> {
            while (!closed.get()) {
                try {
                    PendingChanges changes = pendingChangesQueue.take();
                    pendingChangesBeingProcessed.set(true);
                    System.out.println("Processing pending changes in MerkleTree: " + treeName + ", queue size: " + pendingChangesQueue.size());

                    byte[] key = changes.getKey();
                    byte[] data = changes.getData();

                    System.out.println("1");
                    byte[] existingData = getCommittedData(key);
                    byte[] oldLeafHash = existingData == null ? null : calculateLeafHash(key, existingData);

                    System.out.println("2");
                    // Calculate hash from key and data
                    byte[] newLeafHash = calculateLeafHash(key, data);

                    System.out.println("3");
                    if (oldLeafHash != null && Arrays.equals(oldLeafHash, newLeafHash)) {
                        System.out.println();

                        System.out.println("3-1");
                        //do nothing
                    } else {
                        if (oldLeafHash == null) {

                            System.out.println("3-2");
                            // Key doesn't exist, add new leaf
                            addLeaf(new Node(newLeafHash));

                            System.out.println("3-2-1");
                        } else {
                            // Key exists, update leaf
                            // First get the old leaf hash

                            System.out.println("3-3");
                            updateLeaf(oldLeafHash, newLeafHash);
                            System.out.println("3-3-1");
                        }
                    }


                    System.out.println("4");

                    // Store key-data mapping
                    keyDataCommittedCache.put(new ByteArrayWrapper(key), data);
                    keyDataPendingCache.compute(new ByteArrayWrapper(key), (k, v) -> {
                        if (v == null || Arrays.equals(v, data)) return null;
                        else return v;
                    });
                    System.out.println(keyDataPendingCache.size());
                    if (keyDataPendingCache.size() == 0) {
                        System.out.println(treeName + " has finished pending changes");
                        pendingChangesBeingProcessed.set(false);
                        pendingChangesProcessedEvent.triggerEvent();
                        hasUnsavedChanges.set(false);
                    }
                } catch (Exception e) {
                    logger.error("Error processing pending changes in MerkleTree: " + treeName, e);
                    e.printStackTrace();
                }
            }
        });

        thread.setDaemon(true);
        thread.setName("PendingChangesProcessor-" + treeName);
        thread.start();
    }

    //endregion

    //region ===================== Nested Classes =====================

    /**
     * Represents a single node in the Merkle Tree.
     */
    @Getter
    public class Node {
        private byte[] hash;
        private byte[] left;
        private byte[] right;
        private byte[] parent;

        /**
         * The old hash of the node before it was updated. This is used to delete the old node from the db.
         */
        @Getter
        private byte[] nodeHashToRemoveFromDb = null;

        /**
         * Construct a leaf node with a known hash.
         */
        public Node(byte[] hash) {
            if (hash == null) {
                throw new IllegalArgumentException("Node hash cannot be null");
            }
            this.hash = hash;

            nodesCache.put(new ByteArrayWrapper(hash), this);
        }

        /**
         * Construct a node with all fields.
         */
        public Node(byte[] hash, byte[] left, byte[] right, byte[] parent) {
            if (hash == null) {
                throw new IllegalArgumentException("Node hash cannot be null");
            }
            this.hash = hash;
            this.left = left;
            this.right = right;
            this.parent = parent;

            nodesCache.put(new ByteArrayWrapper(hash), this);
        }

        /**
         * Construct a node (non-leaf) with leftHash and rightHash, auto-calculate node hash.
         */
        public Node(byte[] left, byte[] right) {
            // At least one of left or right must be non-null
            if (left == null && right == null) {
                throw new IllegalArgumentException("At least one of left or right hash must be non-null");
            }

            this.left = left;
            this.right = right;
            this.hash = calculateHash();

            if (this.hash == null) {
                throw new IllegalStateException("Failed to calculate node hash");
            }

            nodesCache.put(new ByteArrayWrapper(hash), this);
        }

        /**
         * Copy constructor for Node.
         */
        public Node(Node node) {
            this.hash = Arrays.copyOf(node.hash, node.hash.length);
            this.left = (node.left != null) ? Arrays.copyOf(node.left, node.left.length) : null;
            this.right = (node.right != null) ? Arrays.copyOf(node.right, node.right.length) : null;
            this.parent = (node.parent != null) ? Arrays.copyOf(node.parent, node.parent.length) : null;
            this.nodeHashToRemoveFromDb = (node.nodeHashToRemoveFromDb != null) ? Arrays.copyOf(node.nodeHashToRemoveFromDb, node.nodeHashToRemoveFromDb.length) : null;
        }

        /**
         * Calculate the hash of this node based on the left and right child hashes.
         */
        public byte[] calculateHash() {

            if (left == null && right == null) {
                // Could be a leaf that hasn't set its hash, or zero-length node
                return null;
            }

            byte[] leftHash = (left != null) ? left : right; // Duplicate if necessary
            byte[] rightHash = (right != null) ? right : left;
            return PWRHash.hash256(leftHash, rightHash);
        }

        /**
         * Encode the node into a byte[] for storage in RocksDB.
         */
        public byte[] encode() {
            boolean hasLeft = (left != null);
            boolean hasRight = (right != null);
            boolean hasParent = (parent != null);

            int length = HASH_LENGTH + 3 // flags for hasLeft, hasRight, hasParent
                    + (hasLeft ? HASH_LENGTH : 0)
                    + (hasRight ? HASH_LENGTH : 0)
                    + (hasParent ? HASH_LENGTH : 0);

            ByteBuffer buffer = ByteBuffer.allocate(length);
            buffer.put(hash);
            buffer.put(hasLeft ? (byte) 1 : (byte) 0);
            buffer.put(hasRight ? (byte) 1 : (byte) 0);
            buffer.put(hasParent ? (byte) 1 : (byte) 0);

            if (hasLeft) buffer.put(left);
            if (hasRight) buffer.put(right);
            if (hasParent) buffer.put(parent);

            return buffer.array();
        }

        /**
         * Set this node's parent.
         */
        public void setParentNodeHash(byte[] parentHash) {
            this.parent = parentHash;
        }

        /**
         * Update this node's hash and propagate the change upward.
         */
        public void updateNodeHash(byte[] newHash) throws RocksDBException {
            //getWriteLock();
            try {
                //We only store the old hash if it is not already set. Because we only need to delete the old node from the db once.
                //New hashes don't have copies in db since they haven't been flushed to disk yet
                if (nodeHashToRemoveFromDb == null) nodeHashToRemoveFromDb = this.hash;

                byte[] oldHash = Arrays.copyOf(this.hash, this.hash.length);
                this.hash = newHash;

                for (Map.Entry<Integer, byte[]> entry : hangingNodes.entrySet()) {
                    if (Arrays.equals(entry.getValue(), oldHash)) {
                        hangingNodes.put(entry.getKey(), newHash);
                        break;
                    }
                }

                nodesCache.remove(new ByteArrayWrapper(oldHash));
                nodesCache.put(new ByteArrayWrapper(newHash), this);

                // Distinguish whether it is a leaf, internal node, or root
                boolean isLeaf = (left == null && right == null);
                boolean isRoot = (parent == null);

                // If this is the root node, update the root hash
                if (isRoot) {
                    rootHash = newHash;

                    if (left != null) {
                        Node leftNode = getNodeByHash(left);
                        if (leftNode != null) {
                            leftNode.setParentNodeHash(newHash);
                        }
                    }

                    if (right != null) {
                        Node rightNode = getNodeByHash(right);
                        if (rightNode != null) {
                            rightNode.setParentNodeHash(newHash);
                        }
                    }
                }

                // If this is a leaf node with a parent, update the parent
                if (isLeaf && !isRoot) {
                    Node parentNode = getNodeByHash(parent);
                    if (parentNode != null) {
                        parentNode.updateLeaf(oldHash, newHash);
                        byte[] newParentHash = parentNode.calculateHash();
                        parentNode.updateNodeHash(newParentHash);
                    }
                }
                // If this is an internal node with a parent, update the parent and children
                else if (!isLeaf && !isRoot) {
                    if (left != null) {
                        Node leftNode = getNodeByHash(left);
                        if (leftNode != null) {
                            leftNode.setParentNodeHash(newHash);
                        }
                    }
                    if (right != null) {
                        Node rightNode = getNodeByHash(right);
                        if (rightNode != null) {
                            rightNode.setParentNodeHash(newHash);
                        }
                    }

                    Node parentNode = getNodeByHash(parent);
                    if (parentNode != null) {
                        parentNode.updateLeaf(oldHash, newHash);
                        byte[] newParentHash = parentNode.calculateHash();
                        parentNode.updateNodeHash(newParentHash);
                    }
                }
            } finally {
                //releaseWriteLock();
            }
        }

        /**
         * Update a leaf (left or right) if it matches the old hash.
         */
        public void updateLeaf(byte[] oldLeafHash, byte[] newLeafHash) {
            if (left != null && Arrays.equals(left, oldLeafHash)) {
                left = newLeafHash;
            } else if (right != null && Arrays.equals(right, oldLeafHash)) {
                right = newLeafHash;
            } else {
                throw new IllegalArgumentException("Old hash not found among this node's children");
            }
        }

        /**
         * Add a leaf to this node (either left or right).
         */
        public void addLeaf(byte[] leafHash) throws RocksDBException {
            if (leafHash == null) {
                throw new IllegalArgumentException("Leaf hash cannot be null");
            }

            Node leafNode = getNodeByHash(leafHash);
            if (leafNode == null) {
                throw new IllegalArgumentException("Leaf node not found: " + Hex.toHexString(leafHash));
            }

            if (left == null) {
                left = leafHash;
            } else if (right == null) {
                right = leafHash;
            } else {
                throw new IllegalArgumentException("Node already has both left and right children");
            }

            byte[] newHash = calculateHash();
            if (newHash == null) {
                throw new IllegalStateException("Failed to calculate new hash after adding leaf");
            }
            updateNodeHash(newHash);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(encode());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Node)) {
                return false;
            }

            Node other = (Node) obj;

            // Compare hash - this is the most important field
            if (this.hash == null && other.hash != null) {
                return false;
            } else if (this.hash != null && other.hash == null) {
                return false;
            } else if (this.hash != null && other.hash != null) {
                if (!Arrays.equals(this.hash, other.hash)) {
                    return false;
                }
            }

            // Compare left child reference
            if (this.left == null && other.left != null) {
                return false;
            } else if (this.left != null && other.left == null) {
                return false;
            } else if (this.left != null && other.left != null) {
                if (!Arrays.equals(this.left, other.left)) {
                    return false;
                }
            }

            // Compare right child reference
            if (this.right == null && other.right != null) {
                return false;
            } else if (this.right != null && other.right == null) {
                return false;
            } else if (this.right != null && other.right != null) {
                if (!Arrays.equals(this.right, other.right)) {
                    return false;
                }
            }

            if (this.parent == null && other.parent != null) {
                return false;
            } else if (this.parent != null && other.parent == null) {
                return false;
            } else if (this.parent != null && other.parent != null) {
                if (!Arrays.equals(this.parent, other.parent)) {
                    return false;
                }
            }

            // Note: We don't compare parent references as they can legitimately differ
            // between source and cloned trees while still representing the same logical node

            return true;
        }
    }

    @Getter
    public class PendingChanges {
        private final byte[] key;
        private final byte[] data;

        public PendingChanges(byte[] key, byte[] data) {
            this.key = key;
            this.data = data;
        }
    }
    //endregion

    public static void main(String[] args) throws Exception {
        MerkleTree tree = new MerkleTree("w1e21115we3/tree1");
        tree.addOrUpdateData("key1".getBytes(), "value1".getBytes());

        MerkleTree tree2 = tree.clone("we2151131we/tree2");

        tree.addOrUpdateData("key2".getBytes(), "value2".getBytes());
        tree.flushToDisk();

        System.out.println("u");
        tree2.update(tree);
        System.out.println("ud");

        tree.flushToDisk();
        tree2.flushToDisk();

        //compare all keys and values of both trees
        List<byte[]> keys1 = tree.getAllKeys();
        List<byte[]> keys2 = tree2.getAllKeys();

        List<byte[]> values1 = tree.getAllData();
        List<byte[]> values2 = tree2.getAllData();

        if (keys1.size() != keys2.size()) {
            System.out.println("Keys size do not match: " + keys1.size() + " != " + keys2.size());
        } else {
            System.out.println("Keys size match: " + keys1.size());
        }

        if (values1.size() != values2.size()) {
            System.out.println("Values size do not match: " + values1.size() + " != " + values2.size());
        } else {
            System.out.println("Values size match: " + values1.size());
        }

        for (int i = 0; i < keys1.size(); i++) {
            byte[] key1 = keys1.get(i);
            byte[] value1 = values1.get(i);

            byte[] key2 = keys2.get(i);
            byte[] value2 = values2.get(i);

            if (!Arrays.equals(key1, key2)) {
                System.out.println("Keys do not match: " + Hex.toHexString(key1) + " != " + Hex.toHexString(key2));
            } else {
                System.out.println("Keys match: " + new String(key1));
            }

            if (!Arrays.equals(value1, value2)) {
                System.out.println("Values do not match: " + Hex.toHexString(value1) + " != " + Hex.toHexString(value2));
            } else {
                System.out.println("Values match: " + new String(value1));
            }
        }

        //sout root hash
        System.out.println("Printing root hash");
        System.out.println("Root hash of tree1: " + Hex.toHexString(tree.getRootHash()));
        System.out.println("Root hash of tree2: " + Hex.toHexString(tree2.getRootHash()));
    }
}
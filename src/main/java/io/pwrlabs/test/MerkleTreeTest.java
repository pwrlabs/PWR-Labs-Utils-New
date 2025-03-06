package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import io.pwrlabs.hashing.PWRHash;
import io.pwrlabs.util.encoders.Hex;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MerkleTreeTest {
    private static int testCount = 0;
    private static int passedCount = 0;

    public static void main(String[] args) {
        try {
            deleteAllTestTrees();
            System.out.println("Running MerkleTree tests...");

            // Run all tests
            testEmptyTree();
            testSingleLeaf();
            testTwoLeaves();
            testLeafUpdate();
            testPersistence();
            testTreeMerge();
            testRevertChanges();
            testErrorCases();
            testHangingNodes();
            
            // New tests
            testAddLeafIfMissing();
            testAddNode();
            testFlushToDisk();
            testComplexTreeMerge();
            testEdgeCases();
            testConcurrentOperations();

            System.out.println("\nTests passed: " + passedCount + "/" + testCount);
        } catch (Exception e) {
            System.out.println("Critical error in test framework: ");
            e.printStackTrace();
        }
    }

    private static void testEmptyTree() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testEmptyTree");
        assertTrue(tree.getNumLeaves() == 0, "Empty tree should have 0 leaves");
        assertNull(tree.getRootHash(), "Empty tree should have null root");
        tree.close();
    }

    private static void testSingleLeaf() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testSingleLeaf");
        byte[] leafHash = createTestHash(1);
        MerkleTree.Node leaf = tree.new Node(leafHash);

        tree.addLeaf(leaf);
        assertEquals(tree.getNumLeaves(), 1, "Should have 1 leaf");
        assertArrayEquals(tree.getRootHash(), leafHash, "Root should match leaf hash");

        tree.close();
    }

    private static void testTwoLeaves() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testTwoLeaves");
        byte[] hash1 = createTestHash(1);
        byte[] hash2 = createTestHash(2);

        tree.addLeaf(tree.new Node(hash1));
        tree.addLeaf(tree.new Node(hash2));

        byte[] expectedRoot = PWRHash.hash256(hash1, hash2);
        assertArrayEquals(tree.getRootHash(), expectedRoot, "Root should be hash of both leaves");
        assertEquals(tree.getNumLeaves(), 2, "Should have 2 leaves");

        tree.close();
    }

    private static void testLeafUpdate() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testLeafUpdate");
        byte[] originalHash = createTestHash(1);
        byte[] newHash = createTestHash(2);

        tree.addLeaf(tree.new Node(originalHash));
        byte[] originalRoot = tree.getRootHash();

        tree.updateLeaf(originalHash, newHash);

        System.out.println("Original root: " + Hex.toHexString(originalRoot));
        System.out.println("Updated root: " + Hex.toHexString(tree.getRootHash()));
        assertFalse(Arrays.equals(tree.getRootHash(), originalRoot),
                "Root should change after leaf update");

        tree.close();
    }

    private static void testPersistence() throws RocksDBException {
        String treeName = "persistenceTest";
        byte[] leafHash = createTestHash(1);

        // Create and flush initial tree
        MerkleTree tree1 = new MerkleTree(treeName);
        tree1.addLeaf(tree1.new Node(leafHash));
        tree1.flushToDisk();
        tree1.close();

        // Reload from disk
        MerkleTree tree2 = new MerkleTree(treeName);
        assertEquals(tree2.getNumLeaves(), 1, "Should persist leaf count");
        assertArrayEquals(tree2.getRootHash(), leafHash, "Should persist root hash");
        tree2.close();
    }

    private static void testTreeMerge() throws RocksDBException {
        MerkleTree source = new MerkleTree("mergeSource");
        MerkleTree target = new MerkleTree("mergeTarget");

        byte[] sourceHash = createTestHash(1);
        source.addLeaf(source.new Node(sourceHash));
        source.flushToDisk();

        target.updateWithTree(source);
        assertArrayEquals(target.getRootHash(), source.getRootHash(),
                "Merged root should match source root");

        source.close();
        target.close();
    }

    private static void testRevertChanges() throws RocksDBException {
        MerkleTree tree = new MerkleTree("revertTest");
        tree.addLeaf(tree.new Node(createTestHash(1)));
        tree.flushToDisk();

        byte[] originalRoot = tree.getRootHash();
        tree.addLeaf(tree.new Node(createTestHash(2))); // Unflushed change
        tree.revertUnsavedChanges();

        assertArrayEquals(tree.getRootHash(), originalRoot, "Should revert to original root");
        tree.close();
    }

    private static void testErrorCases() {
        try {
            MerkleTree tree = new MerkleTree("errorTest");
            tree.updateLeaf(createTestHash(99), createTestHash(100));
            fail("Should throw when updating non-existent leaf");
        } catch (Exception e) {
            // Expected exception
            pass();
        }
    }

    private static void testHangingNodes() throws RocksDBException {
        MerkleTree tree = new MerkleTree("hangingNodesTest");

        // Add 3 leaves to test hanging node handling
        tree.addLeaf(tree.new Node(createTestHash(1)));
        tree.addLeaf(tree.new Node(createTestHash(2)));
        tree.addLeaf(tree.new Node(createTestHash(3)));

        assertEquals(tree.getNumLeaves(), 3, "Should handle odd number of leaves");
        assertNotNull(tree.getRootHash(), "Should maintain valid root with hanging nodes");
        tree.close();
    }

    private static void testAddLeafIfMissing() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testAddLeafIfMissing");
        byte[] leafHash = createTestHash(1);
        
        // Add leaf if missing (should add)
        tree.addLeafIfMissing(leafHash);
        assertEquals(tree.getNumLeaves(), 1, "Should have 1 leaf after adding missing leaf");
        assertArrayEquals(tree.getRootHash(), leafHash, "Root should match leaf hash");
        
        // Add same leaf again (should not add)
        tree.addLeafIfMissing(leafHash);
        assertEquals(tree.getNumLeaves(), 1, "Should still have 1 leaf after adding same leaf again");
        
        // Add different leaf
        byte[] leafHash2 = createTestHash(2);
        tree.addLeafIfMissing(leafHash2);
        assertEquals(tree.getNumLeaves(), 2, "Should have 2 leaves after adding different leaf");
        
        tree.close();
    }

    private static void testAddNode() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testAddNode");
        byte[] leafHash1 = createTestHash(1);
        byte[] leafHash2 = createTestHash(2);
        
        MerkleTree.Node leaf1 = tree.new Node(leafHash1);
        MerkleTree.Node leaf2 = tree.new Node(leafHash2);
        
        // Add node at level 0
        tree.addNode(0, leaf1);
        assertEquals(tree.getNumLeaves(), 0, "Adding node should not increase leaf count");
        assertArrayEquals(tree.getRootHash(), leafHash1, "Root should match node hash");
        
        // Add another node at level 0
        tree.addNode(0, leaf2);
        byte[] expectedRoot = PWRHash.hash256(leafHash1, leafHash2);
        assertArrayEquals(tree.getRootHash(), expectedRoot, "Root should be hash of both nodes");
        
        tree.close();
    }

    private static void testFlushToDisk() throws RocksDBException {
        String treeName = "testFlushToDisk";
        MerkleTree tree1 = new MerkleTree(treeName);
        byte[] leafHash = createTestHash(1);
        
        // Add leaf but don't flush
        tree1.addLeaf(tree1.new Node(leafHash));
        tree1.close();
        
        // Reopen tree and check if changes were saved (they should be, as close() calls flushToDisk())
        MerkleTree tree2 = new MerkleTree(treeName);
        assertEquals(tree2.getNumLeaves(), 1, "Changes should be saved after close");
        
        // Add another leaf and explicitly flush
        byte[] leafHash2 = createTestHash(2);
        tree2.addLeaf(tree2.new Node(leafHash2));
        tree2.flushToDisk();
        tree2.close();
        
        // Reopen and verify both leaves are there
        MerkleTree tree3 = new MerkleTree(treeName);
        assertEquals(tree3.getNumLeaves(), 2, "Both leaves should be saved");
        tree3.close();
    }

    private static void testComplexTreeMerge() throws RocksDBException {
        MerkleTree source = new MerkleTree("complexMergeSource");
        MerkleTree target = new MerkleTree("complexMergeTarget");
        
        // Create a source tree with multiple leaves
        for (int i = 1; i <= 5; i++) {
            source.addLeaf(source.new Node(createTestHash(i)));
        }
        source.flushToDisk();
        
        // Create a target tree with some different leaves
        for (int i = 3; i <= 7; i++) {
            target.addLeaf(target.new Node(createTestHash(i)));
        }
        target.flushToDisk();
        
        // Save target root before merge
        byte[] targetRootBeforeMerge = target.getRootHash();
        
        // Merge source into target
        target.updateWithTree(source);
        
        // Verify target has been updated
        assertFalse(Arrays.equals(target.getRootHash(), targetRootBeforeMerge),
                "Target root should change after merge");
        
        source.close();
        target.close();
    }

    private static void testEdgeCases() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testEdgeCases");
        
        // Test with null leaf hash
        try {
            tree.addLeafIfMissing(null);
            fail("Should throw exception when adding null leaf hash");
        } catch (Exception e) {
            // Expected exception
            pass();
        }
        
        // Test updating non-existent leaf
        try {
            tree.updateLeaf(createTestHash(99), createTestHash(100));
            fail("Should throw when updating non-existent leaf");
        } catch (Exception e) {
            // Expected exception
            pass();
        }
        
        // Test adding leaf with null hash
        try {
            MerkleTree.Node nullNode = tree.new Node(null);
            tree.addLeaf(nullNode);
            fail("Should throw when adding leaf with null hash");
        } catch (Exception e) {
            // Expected exception
            pass();
        }
        
        tree.close();
    }

    private static void testConcurrentOperations() throws Exception {
        final MerkleTree tree = new MerkleTree("testConcurrent");
        final int numThreads = 5;
        final int operationsPerThread = 10;
        
        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        byte[] hash = createTestHash(threadId * 100 + i);
                        tree.addLeafIfMissing(hash);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("Exception in thread " + threadId + ": " + e.getMessage());
                }
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
        
        // Verify the tree has the expected number of leaves
        assertEquals(tree.getNumLeaves(), numThreads * operationsPerThread, 
                "Tree should have all leaves from all threads");
        
        tree.close();
    }

    // Helper methods
    private static byte[] createTestHash(int seed) {
        byte[] hash = new byte[32];
        Arrays.fill(hash, (byte) seed);
        return hash;
    }

    private static void assertTrue(boolean condition, String message) {
        testCount++;
        if (!condition) {
            fail("Assertion failed: " + message);
        }
        passedCount++;
    }

    private static void assertFalse(boolean condition, String message) {
        assertTrue(!condition, message);
    }

    private static void assertNull(Object obj, String message) {
        assertTrue(obj == null, message);
    }

    private static void assertEquals(int actual, int expected, String message) {
        if (actual != expected) {
            fail(message + " (Expected: " + expected + ", Actual: " + actual + ")");
        }
        passedCount++;
        testCount++;
    }

    private static void assertArrayEquals(byte[] a, byte[] b, String message) {
        testCount++;
        if (!Arrays.equals(a, b)) {
            fail("Array mismatch: " + message);
        }
        passedCount++;
    }

    private static void fail(String message) {
        testCount++;
        System.out.println("FAIL: " + message);
        throw new AssertionError(message);
    }

    private static void pass() {
        testCount++;
        passedCount++;
    }

    // Add these methods to the MerkleTreeTest class

    private static void deleteAllTestTrees() {
        String[] treeNames = {
                "testEmptyTree", "testSingleLeaf", "testTwoLeaves", "testLeafUpdate",
                "persistenceTest", "mergeSource", "mergeTarget", "revertTest",
                "errorTest", "hangingNodesTest", "testAddLeafIfMissing", "testAddNode",
                "testFlushToDisk", "complexMergeSource", "complexMergeTarget", 
                "testEdgeCases", "testConcurrent"
        };

        System.out.println("Cleaning up old test trees...");
        for (String treeName : treeNames) {
            deleteTree(treeName);
        }
    }

    private static void deleteTree(String treeName) {
        try {
            // Tree is stored in merkleTree/treeName directory
            File treeDir = new File("merkleTree/" + treeName);
            if (treeDir.exists() && treeDir.isDirectory()) {
                // Delete all files in the directory
                deleteDirectory(treeDir);
                System.out.println("Deleted tree: " + treeName);
            }
        } catch (Exception e) {
            System.out.println("Error deleting tree " + treeName + ": " + e.getMessage());
        }
    }

    private static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        if (!file.delete()) {
                            System.out.println("Could not delete file: " + file.getAbsolutePath());
                        }
                    }
                }
            }
            if (!directory.delete()) {
                System.out.println("Could not delete directory: " + directory.getAbsolutePath());
            }
        }
    }

}

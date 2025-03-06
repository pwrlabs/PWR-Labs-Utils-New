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
                "errorTest", "hangingNodesTest"
        };

        System.out.println("Cleaning up old test trees...");
        for (String treeName : treeNames) {
            deleteTree(treeName);
        }
    }

    private static void deleteTree(String treeName) {
        try {
            // Assume the tree is stored in a directory with the same name
            File treeDir = new File(treeName);
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
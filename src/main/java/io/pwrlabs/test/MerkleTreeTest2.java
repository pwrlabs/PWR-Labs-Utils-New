package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import io.pwrlabs.hashing.PWRHash;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.Arrays;

public class MerkleTreeTest2 {

    public static void main(String[] args) {
        try {
            deleteOldMerkleTrees();

            testInitialization();
            System.out.println("testInitialization passed");

            testAddFirstLeaf();
            System.out.println("testAddFirstLeaf passed");

            testAddSecondLeaf();
            System.out.println("testAddSecondLeaf passed");
            testUpdateLeaf();
            System.out.println("testUpdateLeaf passed");
            testFlushAndReload();
            System.out.println("testFlushAndReload passed");
            testRevertUnsavedChanges();
            System.out.println("testRevertUnsavedChanges passed");
            testUpdateWithTree();
            System.out.println("testUpdateWithTree passed");
            testAddNullLeaf();
            System.out.println("testAddNullLeaf passed");
            System.out.println("All tests passed.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Tests failed: " + e.getMessage());
        }
    }

    private static void testInitialization() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testInitialization");
        try {
            assertEquals(0, tree.getNumLeaves(), "Initial numLeaves should be 0");
            assertNull(tree.getRootHash(), "Initial rootHash should be null");
            assertEquals(0, tree.getDepth(), "Initial depth should be 0");
        } finally {
            tree.close();
        }
    }

    private static void testAddFirstLeaf() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testAddFirstLeaf");
        try {
            byte[] leafHash = new byte[32];
            Arrays.fill(leafHash, (byte) 1);
            MerkleTree.Node leaf = tree.new Node(leafHash);
            tree.addLeaf(leaf);
            assertEquals(1, tree.getNumLeaves(), "numLeaves should be 1");
            assertArrayEquals(leafHash, tree.getRootHash(), "Root hash should be the leaf's hash");
            assertEquals(0, tree.getDepth(), "Depth should be 0 after first leaf");
        } finally {
            tree.close();
        }
    }

    private static void testAddSecondLeaf() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testAddSecondLeaf");
        try {
            byte[] leaf1Hash = new byte[32];
            Arrays.fill(leaf1Hash, (byte) 1);
            MerkleTree.Node leaf1 = tree.new Node(leaf1Hash);
            tree.addLeaf(leaf1);

            byte[] leaf2Hash = new byte[32];
            Arrays.fill(leaf2Hash, (byte) 2);
            MerkleTree.Node leaf2 = tree.new Node(leaf2Hash);
            tree.addLeaf(leaf2);

            assertEquals(2, tree.getNumLeaves(), "numLeaves should be 2");
            assertEquals(1, tree.getDepth(), "Depth should be 1");

            byte[] expectedRoot = PWRHash.hash256(leaf1Hash, leaf2Hash);
            assertArrayEquals(expectedRoot, tree.getRootHash(), "Root hash should be hash of leaf1 and leaf2");
        } finally {
            tree.close();
        }
    }

    private static void testUpdateLeaf() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testUpdateLeaf");
        try {
            byte[] leaf1Hash = new byte[32];
            Arrays.fill(leaf1Hash, (byte) 1);
            MerkleTree.Node leaf1 = tree.new Node(leaf1Hash);
            tree.addLeaf(leaf1);

            byte[] leaf2Hash = new byte[32];
            Arrays.fill(leaf2Hash, (byte) 2);
            MerkleTree.Node leaf2 = tree.new Node(leaf2Hash);
            tree.addLeaf(leaf2);

            byte[] originalRoot = tree.getRootHash();

            byte[] newLeaf1Hash = new byte[32];
            Arrays.fill(newLeaf1Hash, (byte) 3);
            tree.updateLeaf(leaf1Hash, newLeaf1Hash);

            byte[] expectedNewRoot = PWRHash.hash256(newLeaf1Hash, leaf2Hash);
            assertArrayEquals(expectedNewRoot, tree.getRootHash(), "Root hash should update after leaf change");
            assertNotEquals(originalRoot, tree.getRootHash(), "Root hash should change after leaf update");
        } finally {
            tree.close();
        }
    }

    private static void testFlushAndReload() throws RocksDBException {
        String treeName = "testFlushAndReload";
        MerkleTree tree = new MerkleTree(treeName);
        byte[] leafHash = new byte[32];
        try {
            Arrays.fill(leafHash, (byte) 1);
            MerkleTree.Node leaf = tree.new Node(leafHash);
            tree.addLeaf(leaf);
            tree.flushToDisk();
        } finally {
            tree.close();
        }

        MerkleTree reopenedTree = new MerkleTree(treeName);
        try {
            assertEquals(1, reopenedTree.getNumLeaves(), "numLeaves should be 1 after reload");
            assertArrayEquals(leafHash, reopenedTree.getRootHash(), "Root hash should match after reload");
        } finally {
            reopenedTree.close();
        }
    }

    private static void testRevertUnsavedChanges() throws RocksDBException {
        String treeName = "testRevert";
        MerkleTree tree = new MerkleTree(treeName);
        try {
            byte[] leaf1Hash = new byte[32];
            Arrays.fill(leaf1Hash, (byte) 1);
            MerkleTree.Node leaf1 = tree.new Node(leaf1Hash);
            tree.addLeaf(leaf1);
            tree.flushToDisk();

            byte[] leaf2Hash = new byte[32];
            Arrays.fill(leaf2Hash, (byte) 2);
            MerkleTree.Node leaf2 = tree.new Node(leaf2Hash);
            tree.addLeaf(leaf2);
            assertEquals(2, tree.getNumLeaves(), "numLeaves should be 2 before revert");

            tree.revertUnsavedChanges();
            assertEquals(1, tree.getNumLeaves(), "numLeaves should revert to 1");
            assertArrayEquals(leaf1Hash, tree.getRootHash(), "Root should revert to first leaf");
        } finally {
            tree.close();
        }
    }

    private static void testUpdateWithTree() throws RocksDBException {
        MerkleTree sourceTree = new MerkleTree("sourceTree");
        try {
            byte[] sourceLeaf1Hash = new byte[32];
            Arrays.fill(sourceLeaf1Hash, (byte) 1);
            sourceTree.addLeaf(sourceTree.new Node(sourceLeaf1Hash));

            byte[] sourceLeaf2Hash = new byte[32];
            Arrays.fill(sourceLeaf2Hash, (byte) 2);
            sourceTree.addLeaf(sourceTree.new Node(sourceLeaf2Hash));
            sourceTree.flushToDisk();

            MerkleTree targetTree = new MerkleTree("targetTree");
            try {
                byte[] targetLeaf1Hash = new byte[32];
                Arrays.fill(targetLeaf1Hash, (byte) 3);
                targetTree.addLeaf(targetTree.new Node(targetLeaf1Hash));
                targetTree.flushToDisk();

              //  targetTree.updateWithTree(sourceTree);

                assertArrayEquals(sourceTree.getRootHash(), targetTree.getRootHash(), "Root hashes should match after update");
                assertEquals(sourceTree.getNumLeaves(), targetTree.getNumLeaves(), "numLeaves should be " + sourceTree.getNumLeaves() + " after update");
            } finally {
                targetTree.close();
            }
        } finally {
            sourceTree.close();
        }
    }

    private static void testAddNullLeaf() throws RocksDBException {
        MerkleTree tree = new MerkleTree("testAddNullLeaf");
        try {
            try {
                tree.addLeaf(null);
                throw new AssertionError("Adding null leaf should throw IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                // Expected
            }
        } finally {
            tree.close();
        }
    }

    // Helper assertion methods
    private static void assertEquals(int expected, int actual, String message) {
        if (expected != actual) {
            throw new AssertionError(message + " Expected: " + expected + ", Actual: " + actual);
        }
    }

    private static void assertArrayEquals(byte[] expected, byte[] actual, String message) {
        if (!Arrays.equals(expected, actual)) {
            throw new AssertionError(message + " Expected: " + Arrays.toString(expected) + ", Actual: " + Arrays.toString(actual));
        }
    }

    private static void assertNull(Object obj, String message) {
        if (obj != null) {
            throw new AssertionError(message);
        }
    }

    private static void assertNotEquals(byte[] unexpected, byte[] actual, String message) {
        if (Arrays.equals(unexpected, actual)) {
            throw new AssertionError(message);
        }
    }

    //function to delete the merkle tree folder and all its contents
    public static void deleteOldMerkleTrees() {
        File folder = new File("merkleTree");
        deleteFolder(folder);
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}
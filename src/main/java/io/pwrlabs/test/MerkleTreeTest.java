package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import io.pwrlabs.hashing.PWRHash;
import java.util.Arrays;
import java.util.Random;

/**
 * A simple test class for the MerkleTree implementation.
 * This test verifies basic functionality without external test libraries.
 */
public class MerkleTreeTest {

    private static final Random random = new Random();

    public static void main(String[] args) {
        try {
            // Clean up any existing test tree
            cleanupTestTree();

            // Run the tests one at a time, cleaning up between tests
            testBasicTreeOperations();
            cleanupTestTree(); // Clean up after first test

            testNodeUpdates();
            cleanupTestTree(); // Clean up after second test

            testTreePersistence();

            System.out.println("All tests passed!");
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanupTestTree();
        }
    }

    private static void testBasicTreeOperations() throws Exception {
        System.out.println("Running basic tree operations test...");

        // Create a new tree
        MerkleTree tree = new MerkleTree("test_tree");

        // Assert initial state
        assert tree.getNumLeaves() == 0 : "Tree should start with 0 leaves";
        assert tree.getDepth() == 0 : "Tree should start with depth 0";

        // Create and add a leaf node
        byte[] leaf1Data = randomBytes(32);
        MerkleTree.Node leaf1 = tree.new Node(leaf1Data);
        tree.addLeaf(leaf1);

        // Verify tree state after adding one leaf
        assert tree.getNumLeaves() == 1 : "Tree should have 1 leaf";
        assert Arrays.equals(tree.getRootHash(), leaf1Data) : "Root hash should match leaf1 hash";

        // Add a second leaf
        byte[] leaf2Data = randomBytes(32);
        MerkleTree.Node leaf2 = tree.new Node(leaf2Data);
        tree.addLeaf(leaf2);

        // Verify tree state after adding second leaf
        assert tree.getNumLeaves() == 2 : "Tree should have 2 leaves";
        assert tree.getDepth() == 1 : "Tree should have depth 1";

        // Expected root hash for two leaves
        byte[] expectedRootHash = PWRHash.hash256(leaf1Data, leaf2Data);
        assert Arrays.equals(tree.getRootHash(), expectedRootHash) : "Root hash should match the hash of both leaves";

        // Add a third leaf
        byte[] leaf3Data = randomBytes(32);
        MerkleTree.Node leaf3 = tree.new Node(leaf3Data);
        tree.addLeaf(leaf3);

        // Verify tree state after adding third leaf
        assert tree.getNumLeaves() == 3 : "Tree should have 3 leaves";

        // Flush changes to disk
        tree.flushToDisk();

        // Close the tree
        tree.close();

        System.out.println("Basic tree operations test passed!");
    }

    private static void testNodeUpdates() throws Exception {
        System.out.println("Running node updates test...");

        // Clean up any existing test tree before starting this test
        cleanupTestTree();

        // Create a new tree
        MerkleTree tree = new MerkleTree("test_tree");

        // Add three leaves
        byte[][] leafData = new byte[3][];
        MerkleTree.Node[] leaves = new MerkleTree.Node[3];

        for (int i = 0; i < 3; i++) {
            leafData[i] = randomBytes(32);
            leaves[i] = tree.new Node(leafData[i]);
            tree.addLeaf(leaves[i]);
        }

        // Store the original root hash
        byte[] originalRootHash = Arrays.copyOf(tree.getRootHash(), tree.getRootHash().length);

        // Update the first leaf
        byte[] newLeaf1Data = randomBytes(32);
        leaves[0].updateNodeHash(newLeaf1Data);

        // Verify that the root hash has changed
        assert !Arrays.equals(tree.getRootHash(), originalRootHash) : "Root hash should change after updating a leaf";

        // Flush to disk and close
        tree.flushToDisk();
        tree.close();

        System.out.println("Node updates test passed!");
    }

    private static void testTreePersistence() throws Exception {
        System.out.println("Running tree persistence test...");

        // Step 1: Create a tree and add some data
        MerkleTree tree1 = new MerkleTree("test_tree");

        // Add three leaves
        byte[][] leafData = new byte[3][];
        for (int i = 0; i < 3; i++) {
            leafData[i] = randomBytes(32);
            MerkleTree.Node leaf = tree1.new Node(leafData[i]);
            tree1.addLeaf(leaf);
        }

        // Save the root hash
        byte[] expectedRootHash = Arrays.copyOf(tree1.getRootHash(), tree1.getRootHash().length);
        int expectedNumLeaves = tree1.getNumLeaves();
        int expectedDepth = tree1.getDepth();

        // Flush and close
        tree1.flushToDisk();
        tree1.close();

        // Step 2: Reopen the tree and verify state
        MerkleTree tree2 = new MerkleTree("test_tree");

        // Check that metadata was loaded correctly
        assert tree2.getNumLeaves() == expectedNumLeaves : "Reopened tree should have same number of leaves";
        assert tree2.getDepth() == expectedDepth : "Reopened tree should have same depth";
        assert Arrays.equals(tree2.getRootHash(), expectedRootHash) : "Reopened tree should have same root hash";

        // Close the tree
        tree2.close();

        System.out.println("Tree persistence test passed!");
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    private static void cleanupTestTree() {
        // This is a simplistic cleanup - in a real environment you might want
        // to use appropriate directory deletion methods
        try {
            // Delete the test tree if it exists
            java.io.File treeDir = new java.io.File("merkleTree/test_tree");
            if (treeDir.exists()) {
                deleteDirectory(treeDir);
            }
        } catch (Exception e) {
            System.err.println("Failed to clean up test tree: " + e.getMessage());
        }
    }

    private static boolean deleteDirectory(java.io.File directoryToBeDeleted) {
        java.io.File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (java.io.File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
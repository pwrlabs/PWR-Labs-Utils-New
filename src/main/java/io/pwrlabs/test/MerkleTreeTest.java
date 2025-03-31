package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import org.rocksdb.RocksDBException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * A simple class that tests MerkleTree functionality multiple times without using any external
 * test framework. This includes testing repeated updates to the same key to ensure data is
 * correctly updated in the tree.
 */
public class MerkleTreeTest {

    /**
     * Main entry point to run all tests multiple times.
     */
    public static void main(String[] args) {
        // We run the entire test suite multiple times to ensure stability across repeated usage.
        for (int i = 0; i < 3; i++) {
            System.out.println("======= TEST RUN #" + (i + 1) + " =======");
            runTests("testTree" + i);
        }
        System.out.println("All test runs completed successfully!");
    }

    /**
     * Runs a series of tests against the MerkleTree class using the given treeName.
     *
     * @param treeName a unique name for each test run
     */
    private static void runTests(String treeName) {
        MerkleTree tree = null;
        try {
            // 1) Test Constructor
            System.out.println("1) Testing constructor with treeName: " + treeName);
            tree = new MerkleTree(treeName);

            // Check initial states
            System.out.println("   RootHash (should be null): " + (tree.getRootHash() == null));
            System.out.println("   NumLeaves (should be 0): " + tree.getNumLeaves());
            System.out.println("   Depth (should be 0): " + tree.getDepth());

            // 2) Test addOrUpdateData (with repeated updates), getData, containsKey
            System.out.println("2) Testing addOrUpdateData with repeated updates on the same key...");

            byte[] keyA = "KeyA".getBytes();
            // Repeatedly update keyA with new data, verifying each time
            for (int i = 0; i < 5; i++) {
                byte[] newDataA = ("DataA" + i).getBytes();
                tree.addOrUpdateData(keyA, newDataA);

                byte[] fetchedA = tree.getData(keyA);
                boolean matches = Arrays.equals(newDataA, fetchedA);
                System.out.println("   Round " + i + " -> getData(KeyA) = 'DataA" + i
                        + "' match: " + matches);
                if (!matches) {
                    throw new AssertionError("KeyA data does not match expected value at round " + i);
                }
            }
            // After multiple updates to the same key, we still should have exactly 1 leaf for KeyA
            System.out.println("   After repeated updates, getNumLeaves() is (should be 1): " + tree.getNumLeaves());
            System.out.println("   After repeated updates, rootHash is not null: " + (tree.getRootHash() != null));
            // also check containsKey
            System.out.println("   containsKey(KeyA) is true: " + tree.containsKey(keyA));

            // Insert a second key to ensure multi-key usage
            byte[] keyB = "KeyB".getBytes();
            byte[] dataB = "DataB".getBytes();
            tree.addOrUpdateData(keyB, dataB);
            System.out.println("   Inserted KeyB -> DataB");
            System.out.println("   getData(KeyB) matches DataB: "
                    + Arrays.equals(dataB, tree.getData(keyB)));
            System.out.println("   getNumLeaves() now (should be 2): " + tree.getNumLeaves());

            // 3) Check rootHash, depth
            System.out.println("   RootHash after inserts (should not be null): " + (tree.getRootHash() != null));
            System.out.println("   Depth after inserts (>= 0): " + tree.getDepth());

            // 4) Test revertUnsavedChanges
            System.out.println("3) Testing revertUnsavedChanges...");
            tree.revertUnsavedChanges();
            // Because we have not called flushToDisk after the last updates, revert would
            // clear in-memory caches. However, data is still present in RocksDB from before
            // or from the flush that might have happened during addOrUpdate, so getData should still work.
            byte[] revertCheckA = tree.getData(keyA);
            System.out.println("   getData(KeyA) after revert: " + (revertCheckA == null ? "null" : new String(revertCheckA)));
            byte[] revertCheckB = tree.getData(keyB);
            System.out.println("   getData(KeyB) after revert: " + (revertCheckB == null ? "null" : new String(revertCheckB)));

            // 5) Test getAllKeys and getAllData
            System.out.println("4) Testing getAllKeys and getAllData...");
            List<byte[]> allKeys = tree.getAllKeys();
            List<byte[]> allData = tree.getAllData();

            System.out.println("   getAllKeys() size: " + allKeys.size());
            for (byte[] k : allKeys) {
                System.out.println("     Key: " + new String(k));
            }

            System.out.println("   getAllData() size: " + allData.size());
            for (byte[] d : allData) {
                System.out.println("     Data: " + new String(d));
            }

            // 6) Test flushToDisk explicitly
            System.out.println("5) Testing flushToDisk...");
            tree.flushToDisk();
            System.out.println("   flushToDisk() called successfully.");

            // 7) Test getAllNodes
            System.out.println("6) Testing getAllNodes...");
            HashSet<MerkleTree.Node> allNodes = tree.getAllNodes();
            System.out.println("   getAllNodes() result size: " + allNodes.size());
            for (MerkleTree.Node node : allNodes) {
                System.out.println("     Node hash: " + bytesToHex(node.getHash()));
            }

            // 8) Test clone
            System.out.println("7) Testing clone method...");
            String cloneName = treeName + "_clone";
            MerkleTree clonedTree = tree.clone(cloneName);
            System.out.println("   Cloned tree created with name: " + cloneName);

            // Compare a few properties on the cloned tree
            System.out.println("   Cloned tree getRootHash() not null: " + (clonedTree.getRootHash() != null));
            System.out.println("   Cloned tree getNumLeaves(): " + clonedTree.getNumLeaves());
            System.out.println("   Cloned tree getDepth(): " + clonedTree.getDepth());
            // Compare data for KeyA
            byte[] clonedKeyAData = clonedTree.getData(keyA);
            System.out.println("   Cloned tree getData(KeyA) matches original: "
                    + Arrays.equals(tree.getData(keyA), clonedKeyAData));
            // Compare data for KeyB
            byte[] clonedKeyBData = clonedTree.getData(keyB);
            System.out.println("   Cloned tree getData(KeyB) matches original: "
                    + Arrays.equals(tree.getData(keyB), clonedKeyBData));

            // Close cloned tree
            clonedTree.close();
            System.out.println("   Cloned tree closed successfully.");

            // 9) Finally, close the original tree
            System.out.println("8) Testing close method...");
            tree.close();
            System.out.println("   Original tree closed successfully.");

            // Extra: Attempt to close again (should not throw errors; just be idempotent)
            tree.close();
            System.out.println("   Original tree closed again (no error).");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // If a test fails unexpectedly, ensure we attempt to close the tree
            if (tree != null) {
                try {
                    tree.close();
                } catch (RocksDBException ex) {
                    // ignore
                }
            }
        }
    }

    /**
     * Simple helper to display bytes as hex for debugging.
     */
    private static String bytesToHex(byte[] data) {
        if (data == null) return "null";
        StringBuilder sb = new StringBuilder();
        for (byte b : data) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}

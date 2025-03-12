package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class MerkleTreeTest {
    private static int testCount = 0;

    public static void main(String[] args) {
        testEmptyTree();
        testSingleLeaf();
        testMultipleLeaves();
        testUpdateLeaf();
        testHangingNodes();
        testKeyDataIntegrity();
        testMetadataIntegrity();
        testModifyAfterClone();
    }

    private static void testEmptyTree() {
        testCount++;
        try {
            MerkleTree original = new MerkleTree("testEmpty_" + testCount);
            MerkleTree clone = original.clone("testEmptyClone_" + testCount);

            assertTrue(original.equals(clone), "Empty trees should be equal");
            assertTrue(clone.getNumLeaves() == 0, "Clone should have 0 leaves");

            original.close();
            clone.close();
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void testSingleLeaf() {
        testCount++;
        try {
            MerkleTree original = new MerkleTree("testSingle_" + testCount);
            original.addOrUpdateData("key1".getBytes(), "value1".getBytes());
            original.flushToDisk();

            MerkleTree clone = original.clone("testSingleClone_" + testCount);

            assertTrue(original.equals(clone), "Single leaf trees should be equal");
            assertTrue(clone.containsKey("key1".getBytes()), "Clone should contain key");
            assertEqualArrays("value1".getBytes(), clone.getData("key1".getBytes()),
                    "Clone data should match");

            original.close();
            clone.close();
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void testMultipleLeaves() {
        testCount++;
        try {
            MerkleTree original = new MerkleTree("testMultiple_" + testCount);
            for (int i = 0; i < 10; i++) {
                original.addOrUpdateData(("key" + i).getBytes(), ("value" + i).getBytes());
            }
            original.flushToDisk();

            MerkleTree clone = original.clone("testMultipleClone_" + testCount);

            assertTrue(original.equals(clone), "Multi-leaf trees should be equal");

            List<byte[]> originalKeys = original.getAllKeys();
            List<byte[]> cloneKeys = clone.getAllKeys();
            assertTrue(originalKeys.size() == cloneKeys.size(), "Key count should match");

            for (byte[] key : originalKeys) {
                assertTrue(clone.containsKey(key), "Clone should contain all keys");
                assertEqualArrays(original.getData(key), clone.getData(key),
                        "Data should match for key: " + new String(key));
            }

            original.close();
            clone.close();
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void testUpdateLeaf() {
        testCount++;
        try {
            MerkleTree original = new MerkleTree("testUpdate_" + testCount);
            original.addOrUpdateData("key".getBytes(), "value1".getBytes());
            original.flushToDisk();

            // Update value
            original.addOrUpdateData("key".getBytes(), "value2".getBytes());
            original.flushToDisk();

            MerkleTree clone = original.clone("testUpdateClone_" + testCount);

            assertTrue(original.equals(clone), "Updated trees should be equal");
            assertEqualArrays("value2".getBytes(), clone.getData("key".getBytes()),
                    "Clone should have updated value");

            original.close();
            clone.close();
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void testHangingNodes() {
        testCount++;
        try {
            MerkleTree original = new MerkleTree("testHanging_" + testCount);
            // Add 3 leaves to create hanging nodes
            original.addOrUpdateData("key1".getBytes(), "value1".getBytes());
            original.addOrUpdateData("key2".getBytes(), "value2".getBytes());
            original.addOrUpdateData("key3".getBytes(), "value3".getBytes());
            original.flushToDisk();

            MerkleTree clone = original.clone("testHangingClone_" + testCount);

            assertTrue(original.equals(clone), "Trees with hanging nodes should be equal");
            assertTrue(original.getDepth() == clone.getDepth(), "Depth should match");
            assertTrue(original.getNumLeaves() == clone.getNumLeaves(), "Leaf count should match");

            original.close();
            clone.close();
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void testKeyDataIntegrity() {
        testCount++;
        try {
            MerkleTree original = new MerkleTree("testDataIntegrity_" + testCount);
            for (int i = 0; i < 100; i++) {
                original.addOrUpdateData(("dataKey" + i).getBytes(), ("dataValue" + i).getBytes());
            }
            original.flushToDisk();

            MerkleTree clone = original.clone("testDataIntegrityClone_" + testCount);

            List<byte[]> originalData = original.getAllData();
            List<byte[]> cloneData = clone.getAllData();
            assertTrue(originalData.size() == cloneData.size(), "Data count should match");

            for (int i = 0; i < originalData.size(); i++) {
                assertEqualArrays(originalData.get(i), cloneData.get(i),
                        "Data content should match");
            }

            original.close();
            clone.close();
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void testMetadataIntegrity() {
        testCount++;
        try {
            MerkleTree original = new MerkleTree("testMetadata_" + testCount);
            for (int i = 0; i < 5; i++) {
                original.addOrUpdateData(("metaKey" + i).getBytes(), ("metaValue" + i).getBytes());
            }
            original.flushToDisk();

            MerkleTree clone = original.clone("testMetadataClone_" + testCount);

            assertEqualArrays(original.getRootHash(), clone.getRootHash(), "Root hash should match");
            assertTrue(original.getNumLeaves() == clone.getNumLeaves(), "Leaf count should match");
            assertTrue(original.getDepth() == clone.getDepth(), "Depth should match");

            original.close();
            clone.close();
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void testModifyAfterClone() {
        testCount++;
        try {
            MerkleTree original = new MerkleTree("testModify_" + testCount);
            original.addOrUpdateData("key".getBytes(), "original".getBytes());
            original.flushToDisk();

            MerkleTree clone = original.clone("testModifyClone_" + testCount);

            // Modify original after cloning
            original.addOrUpdateData("key".getBytes(), "modified".getBytes());
            original.flushToDisk();

            assertFalse(Arrays.equals(
                    original.getData("key".getBytes()),
                    clone.getData("key".getBytes())
            ), "Clone should not be affected by original modifications");

            original.close();
            clone.close();
        } catch (Exception e) {
            handleException(e);
        }
    }

    // Helper methods
    private static void assertTrue(boolean condition, String message) {
        if (condition) {
            System.out.println("[PASS] " + message);
        } else {
            System.out.println("[FAIL] " + message);
        }
    }

    private static void assertFalse(boolean condition, String message) {
        assertTrue(!condition, message);
    }

    private static void assertEqualArrays(byte[] arr1, byte[] arr2, String message) {
        boolean equal = Arrays.equals(arr1, arr2);
        if (equal) {
            System.out.println("[PASS] " + message);
        } else {
            System.out.println("[FAIL] " + message);
        }
    }

    private static void handleException(Exception e) {
        System.out.println("[ERROR] Test failed with exception: " + e.getMessage());
        e.printStackTrace();
    }
}
package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import io.pwrlabs.hashing.PWRHash;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.Arrays;

public class MerkleTreeDatabaseTest {

    public static void main(String[] args) {
        try {
            deleteOldMerkleTrees();
            
            testAddAndGetData();
            System.out.println("testAddAndGetData passed");
            
            testUpdateData();
            System.out.println("testUpdateData passed");
            
            testTreeUpdateWithData();
            System.out.println("testTreeUpdateWithData passed");
            
            testDifferentSizeTreeUpdateWithData();
            System.out.println("testDifferentSizeTreeUpdateWithData passed");
            
            System.out.println("All tests passed.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Tests failed: " + e.getMessage());
        }
    }
    
    // Delete old merkle trees
    public static void deleteOldMerkleTrees() {
        File folder = new File("merkleTree");
        deleteFolder(folder);
    }
    
    public static void deleteFolder(File folder) {
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if(files!=null) {
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
    
    private static void testAddAndGetData() throws RocksDBException {
        System.out.println("Testing adding and retrieving data...");
        
        MerkleTree tree = new MerkleTree("testAddGetData");
        try {
            // Add some key-value pairs
            byte[] key1 = "key1".getBytes();
            byte[] data1 = "data1".getBytes();
            tree.addOrUpdateData(key1, data1);
            
            byte[] key2 = "key2".getBytes();
            byte[] data2 = "data2".getBytes();
            tree.addOrUpdateData(key2, data2);
            
            // Retrieve and verify data
            byte[] retrievedData1 = tree.getData(key1);
            if (!Arrays.equals(data1, retrievedData1)) {
                throw new AssertionError("Retrieved data doesn't match original data for key1");
            }
            
            byte[] retrievedData2 = tree.getData(key2);
            if (!Arrays.equals(data2, retrievedData2)) {
                throw new AssertionError("Retrieved data doesn't match original data for key2");
            }
            
            // Try to retrieve non-existent key
            byte[] nonExistentKey = "nonExistentKey".getBytes();
            byte[] retrievedData3 = tree.getData(nonExistentKey);
            if (retrievedData3 != null) {
                throw new AssertionError("Retrieved data for non-existent key should be null");
            }
        } finally {
            tree.close();
        }
    }
    
    private static void testUpdateData() throws RocksDBException {
        System.out.println("Testing updating data...");
        
        MerkleTree tree = new MerkleTree("testUpdateData");
        try {
            // Add a key-value pair
            byte[] key = "key".getBytes();
            byte[] originalData = "originalData".getBytes();
            tree.addOrUpdateData(key, originalData);
            
            // Update the data
            byte[] updatedData = "updatedData".getBytes();
            tree.addOrUpdateData(key, updatedData);
            
            // Retrieve and verify data
            byte[] retrievedData = tree.getData(key);
            if (!Arrays.equals(updatedData, retrievedData)) {
                throw new AssertionError("Retrieved data doesn't match updated data");
            }
        } finally {
            tree.close();
        }
    }
    
    private static void testTreeUpdateWithData() throws RocksDBException {
        System.out.println("Testing tree update with data...");
        
        // Create source tree with data
        MerkleTree sourceTree = new MerkleTree("sourceTreeWithData");
        try {
            // Add some key-value pairs to source tree
            for (int i = 1; i <= 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] data = ("data" + i).getBytes();
                sourceTree.addOrUpdateData(key, data);
            }
            
            // Create target tree
            MerkleTree targetTree = new MerkleTree("targetTreeWithData");
            try {
                // Update target tree with source tree
              //  targetTree.updateWithTree(sourceTree);
                
                // Verify data was copied
                for (int i = 1; i <= 10; i++) {
                    byte[] key = ("key" + i).getBytes();
                    byte[] expectedData = ("data" + i).getBytes();
                    byte[] retrievedData = targetTree.getData(key);
                    
                    if (!Arrays.equals(expectedData, retrievedData)) {
                        throw new AssertionError("Data not copied correctly for key " + i);
                    }
                }
            } finally {
                targetTree.close();
            }
        } finally {
            sourceTree.close();
        }
    }
    
    private static void testDifferentSizeTreeUpdateWithData() throws RocksDBException {
        System.out.println("Testing update of trees with different sizes and data...");
        
        // Create source tree with 45 key-value pairs
        MerkleTree sourceTree = new MerkleTree("largeSourceTreeWithData");
        try {
            // Add 45 key-value pairs to source tree
            for (int i = 1; i <= 45; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] data = ("data" + i).getBytes();
                sourceTree.addOrUpdateData(key, data);
            }
            
            // Create target tree with 30 key-value pairs
            MerkleTree targetTree = new MerkleTree("largeTargetTreeWithData");
            try {
                // Add 30 different key-value pairs to target tree
                for (int i = 101; i <= 130; i++) {
                    byte[] key = ("key" + i).getBytes();
                    byte[] data = ("data" + i).getBytes();
                    targetTree.addOrUpdateData(key, data);
                }
                
                // Update target tree with source tree
              //  targetTree.updateWithTree(sourceTree);
                
                // Verify data was copied
                for (int i = 1; i <= 45; i++) {
                    byte[] key = ("key" + i).getBytes();
                    byte[] expectedData = ("data" + i).getBytes();
                    byte[] retrievedData = targetTree.getData(key);
                    
                    if (!Arrays.equals(expectedData, retrievedData)) {
                        throw new AssertionError("Data not copied correctly for key " + i);
                    }
                }
            } finally {
                targetTree.close();
            }
        } finally {
            sourceTree.close();
        }
    }
}

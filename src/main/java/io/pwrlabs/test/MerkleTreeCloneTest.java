package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.Arrays;
import java.util.Set;

public class MerkleTreeCloneTest {

    public static void main(String[] args) {
        try {
            System.out.println("Starting MerkleTreeCloneTest...");
            deleteOldMerkleTrees();
            System.out.println("Old Merkle trees deleted");
            
            // Run only the first test for now
            testCloneEmptyTree();
            System.out.println("testCloneEmptyTree passed");

            testCloneTreeWithLeaves();
            System.out.println("testCloneTreeWithLeaves passed");

            testCloneTreeWithKeyValueData();
            System.out.println("testCloneTreeWithKeyValueData passed");

            testCloneExistingTree();
            System.out.println("testCloneExistingTree passed");
            
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
    
    private static byte[] createTestHash(int seed) {
        byte[] hash = new byte[32];
        Arrays.fill(hash, (byte) seed);
        return hash;
    }
    
    private static void testCloneEmptyTree() throws RocksDBException {
        System.out.println("Testing cloning an empty tree...");
        
        // Create an empty source tree
        MerkleTree sourceTree = new MerkleTree("emptySourceTree");
        try {
            // Clone the tree
            MerkleTree clonedTree = sourceTree.clone("emptyClonedTree");
            try {
                // Verify the cloned tree is also empty
                if (clonedTree.getRootHash() != null) {
                    throw new AssertionError("Cloned tree should be empty");
                }
                
                if (clonedTree.getNumLeaves() != 0) {
                    throw new AssertionError("Cloned tree should have 0 leaves");
                }
                
                if (clonedTree.getDepth() != 0) {
                    throw new AssertionError("Cloned tree should have depth 0");
                }
            } finally {
                clonedTree.close();
            }
        } finally {
            sourceTree.close();
            System.out.println("sourceTree closed");
        }
    }
    
    private static void testCloneTreeWithLeaves() throws RocksDBException {
        System.out.println("Testing cloning a tree with leaves...");
        
        // Create a source tree with leaves
        MerkleTree sourceTree = new MerkleTree("sourceTreeWithLeaves");
        try {
            // Add 10 leaves to the source tree
            for (int i = 1; i <= 10; i++) {
                byte[] leafHash = createTestHash(i);
                sourceTree.addLeaf(sourceTree.new Node(leafHash));
            }
            sourceTree.flushToDisk();
            
            // Clone the tree
            MerkleTree clonedTree = sourceTree.clone("clonedTreeWithLeaves");
            try {
                // Verify the cloned tree has the same structure
                if (!Arrays.equals(sourceTree.getRootHash(), clonedTree.getRootHash())) {
                    throw new AssertionError("Root hashes don't match");
                }
                
                if (sourceTree.getNumLeaves() != clonedTree.getNumLeaves()) {
                    throw new AssertionError("Number of leaves doesn't match");
                }
                
                if (sourceTree.getDepth() != clonedTree.getDepth()) {
                    throw new AssertionError("Tree depth doesn't match");
                }
                
                // Compare all nodes between trees
                Set<MerkleTree.Node> sourceNodes = sourceTree.getAllNodes();
                Set<MerkleTree.Node> clonedNodes = clonedTree.getAllNodes();
                
                if (sourceNodes.size() != clonedNodes.size()) {
                    throw new AssertionError("Number of nodes doesn't match");
                }
                
                for (MerkleTree.Node node : sourceNodes) {
                    if (!clonedNodes.contains(node)) {
                        throw new AssertionError("Node missing in cloned tree");
                    }
                }
            } finally {
                clonedTree.close();
            }
        } finally {
            sourceTree.close();
        }
    }
    
    private static void testCloneTreeWithKeyValueData() throws RocksDBException {
        System.out.println("Testing cloning a tree with key-value data...");
        
        // Create a source tree with key-value data
        MerkleTree sourceTree = new MerkleTree("sourceTreeWithData");
        try {
            // Add some key-value pairs
            for (int i = 1; i <= 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] data = ("data" + i).getBytes();
                sourceTree.addOrUpdateData(key, data);
            }
            sourceTree.flushToDisk();
            
            // Clone the tree
            MerkleTree clonedTree = sourceTree.clone("clonedTreeWithData");
            try {
                // Verify the key-value data was copied
                for (int i = 1; i <= 10; i++) {
                    byte[] key = ("key" + i).getBytes();
                    byte[] expectedData = ("data" + i).getBytes();
                    byte[] retrievedData = clonedTree.getData(key);
                    
                    if (!Arrays.equals(expectedData, retrievedData)) {
                        throw new AssertionError("Data not copied correctly for key " + i);
                    }
                }
            } finally {
                clonedTree.close();
            }
        } finally {
            sourceTree.close();
        }
    }
    
    private static void testCloneExistingTree() throws RocksDBException {
        System.out.println("Testing cloning to an existing tree location...");
        
        // Create a source tree
        MerkleTree sourceTree = new MerkleTree("sourceTreeForExisting");
        try {
            // Add some leaves to the source tree
            for (int i = 1; i <= 5; i++) {
                byte[] leafHash = createTestHash(i);
                sourceTree.addLeaf(sourceTree.new Node(leafHash));
            }
            sourceTree.flushToDisk();
            
            // Create an existing tree at the target location
            MerkleTree existingTree = new MerkleTree("existingTargetTree");
            try {
                // Add different leaves to the existing tree
                for (int i = 101; i <= 105; i++) {
                    byte[] leafHash = createTestHash(i);
                    existingTree.addLeaf(existingTree.new Node(leafHash));
                }
                existingTree.flushToDisk();
                
                // Remember the root hash of the existing tree
                byte[] existingRootHash = existingTree.getRootHash();
                
                // Close the existing tree to release resources
                existingTree.close();
                
                // Clone the source tree to the same location
                MerkleTree clonedTree = sourceTree.clone("existingTargetTree");
                try {
                    // Verify the cloned tree has the source tree's structure, not the existing tree's
                    if (!Arrays.equals(sourceTree.getRootHash(), clonedTree.getRootHash())) {
                        throw new AssertionError("Root hashes don't match source tree");
                    }
                    
                    if (Arrays.equals(existingRootHash, clonedTree.getRootHash())) {
                        throw new AssertionError("Cloned tree still has the existing tree's root hash");
                    }
                    
                    if (sourceTree.getNumLeaves() != clonedTree.getNumLeaves()) {
                        throw new AssertionError("Number of leaves doesn't match source tree");
                    }
                } finally {
                    clonedTree.close();
                }
            } catch (Exception e) {
                // If the existing tree is already closed by the test, this is expected
                if (!e.getMessage().contains("closed")) {
                    throw e;
                }
            }
        } finally {
            sourceTree.close();
        }
    }
}

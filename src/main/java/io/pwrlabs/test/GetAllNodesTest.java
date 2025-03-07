package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.Arrays;

public class GetAllNodesTest {

    public static void main(String[] args) {
        try {
            deleteOldMerkleTrees();
            
            System.out.println("Testing getAllNodes() method...");
            
            // Create a tree with 10 leaves
            MerkleTree tree = new MerkleTree("getAllNodesTest");
            try {
                // Add 10 leaves to the tree
                for (int i = 1; i <= 10; i++) {
                    byte[] leafHash = createTestHash(i);
                    tree.addLeaf(tree.new Node(leafHash));
                }
                tree.flushToDisk();
                
                // Get all nodes from the tree
                var nodes = tree.getAllNodes();
                
                // Print the number of nodes
                System.out.println("Tree has " + nodes.size() + " nodes");
                
                // Verify that the number of nodes is correct
                // For a tree with 10 leaves, we should have:
                // - 10 leaf nodes
                // - 5 internal nodes at level 1 (pairing the 10 leaves)
                // - 3 internal nodes at level 2 (pairing the 5 nodes at level 1)
                // - 2 internal nodes at level 3
                // - 1 root node at level 4
                // Total: 21 nodes
                int expectedNodeCount = 21;
                if (nodes.size() == expectedNodeCount) {
                    System.out.println("PASS: Tree has the expected number of nodes: " + nodes.size());
                } else {
                    System.out.println("FAIL: Tree has " + nodes.size() + " nodes, expected " + expectedNodeCount);
                    System.exit(1);
                }
                
                // Verify that the root hash is included in the nodes
                byte[] rootHash = tree.getRootHash();
                boolean rootFound = false;
                for (var node : nodes) {
                    if (Arrays.equals(node.getHash(), rootHash)) {
                        rootFound = true;
                        break;
                    }
                }
                
                if (rootFound) {
                    System.out.println("PASS: Root hash found in nodes");
                } else {
                    System.out.println("FAIL: Root hash not found in nodes");
                    System.exit(1);
                }
                
                System.out.println("getAllNodes() test passed");
            } finally {
                tree.close();
            }
            
        } catch (Exception e) {
            System.out.println("Test failed with exception: ");
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static byte[] createTestHash(int seed) {
        byte[] hash = new byte[32];
        Arrays.fill(hash, (byte) seed);
        return hash;
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
}

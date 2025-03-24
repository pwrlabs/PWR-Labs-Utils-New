package io.pwrlabs.utils;

import java.util.HashMap;
import java.util.Map;

public class BinaryJSONKeyMapper {
    private static final Map<String, Short> KEY_TO_ID = new HashMap<>();
    private static final Map<Short, String> ID_TO_KEY = new HashMap<>();
    
    private static short nextId = 0;
    
    public static void addKey(String key) {
        key = key.toLowerCase();

        KEY_TO_ID.put(key, nextId);
        ID_TO_KEY.put(nextId++, key);
    }

    public static Short getId(String key) {
        return KEY_TO_ID.get(key);
    }

    public static String getKey(short id) {
        return ID_TO_KEY.get(id);
    }

    public static void removeKey(String key) {
        key = key.toLowerCase();
        short id = KEY_TO_ID.get(key);

        KEY_TO_ID.remove(key);
        ID_TO_KEY.remove(id);
    }

    public static void removeKey(short id) {
        String key = ID_TO_KEY.get(id);
        KEY_TO_ID.remove(key);
        ID_TO_KEY.remove(id);
    }

    public static boolean containsKey(String key) {
        return KEY_TO_ID.containsKey(key);
    }

    public static boolean containsKey(short id) {
        return ID_TO_KEY.containsKey(id);
    }

    /**
     * Returns the key that most closely matches the input key based on Levenshtein distance.
     * @param key The key to find the closest match for
     * @return The closest matching key from the existing keys, or null if no keys exist
     */
    public static String closestMatch(String key) {
        if (key == null || key.isEmpty() || KEY_TO_ID.isEmpty()) {
            return null;
        }

        key = key.toLowerCase(); // Consistent with other methods in the class

        // If the key exists exactly, return it
        if (KEY_TO_ID.containsKey(key)) {
            return key;
        }

        String closestKey = null;
        int minDistance = Integer.MAX_VALUE;

        // Find the key with minimum Levenshtein distance
        for (String existingKey : KEY_TO_ID.keySet()) {
            int distance = levenshteinDistance(key, existingKey);
            if (distance < minDistance) {
                minDistance = distance;
                closestKey = existingKey;
            }
        }

        return closestKey;
    }

    /**
     * Calculates the Levenshtein distance between two strings.
     * @param s1 First string
     * @param s2 Second string
     * @return The minimum number of single-character edits needed to change one string into the other
     */
    private static int levenshteinDistance(String s1, String s2) {
        int[][] dp = new int[s1.length() + 1][s2.length() + 1];

        for (int i = 0; i <= s1.length(); i++) {
            dp[i][0] = i;
        }

        for (int j = 0; j <= s2.length(); j++) {
            dp[0][j] = j;
        }

        for (int i = 1; i <= s1.length(); i++) {
            for (int j = 1; j <= s2.length(); j++) {
                int cost = (s1.charAt(i - 1) == s2.charAt(j - 1)) ? 0 : 1;
                dp[i][j] = Math.min(
                        Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1),
                        dp[i - 1][j - 1] + cost
                );
            }
        }

        return dp[s1.length()][s2.length()];
    }

    static {
        addKey("sender");
        addKey("receiver");
        addKey("transactionHash");
        addKey("nonce");
        addKey("amount");
        addKey("vidaId");
        addKey("data");
        addKey("feePerByte");
        addKey("paidActionFee");
        addKey("paidTotalFee");
        addKey("success");
        addKey("errorMessage");

        addKey("blockNumber");
        addKey("timeStamp");
        addKey("blockHash");
        addKey("rootHash");
        addKey("proposer");
        addKey("previousBlockHash");
        addKey("blockchainVersion");
        addKey("blockReward");

        addKey("title");
        addKey("description");
        addKey("proposalHash");
        addKey("earlyWithdrawTime");
        addKey("earlyWithdrawPenalty");
        addKey("maxBlockSize");
        addKey("maxTransactionSize");
        addKey("overallBurnPercentage");
        addKey("rewardPerYear");
        addKey("validatorCountLimit");
        addKey("validatorJoiningFee");
        addKey("vidaIdClaimingFee");
        addKey("vmOwnerTransactionFeeShare");
        addKey("vote");
        addKey("transactions");
        addKey("guardianAddress");
        addKey("guardianExpiryDate");
        addKey("ip");
        addKey("validatorAddress");
        addKey("fromValidatorAddress");
        addKey("toValidatorAddress");
        addKey("sharesAmount");
        addKey("addresses");
        addKey("conduits");
        addKey("conduitThreshold");
        addKey("mode");
        addKey("vidaConduitPowers");
        addKey("isPrivate");
        addKey("publicKey");
    }
}

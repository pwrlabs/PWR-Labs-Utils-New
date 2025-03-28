package io.pwrlabs.utils;

import java.security.PublicKey;
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

    //region ==================== Constants ====================
    public static final String SENDER = "sender";
    public static final String RECEIVER = "receiver";
    public static final String TRANSACTION_HASH = "transactionHash";
    public static final String NONCE = "nonce";
    public static final String AMOUNT = "amount";
    public static final String VIDA_ID = "vidaId";
    public static final String DATA = "data";
    public static final String FEE_PER_BYTE = "feePerByte";
    public static final String PAID_ACTION_FEE = "paidActionFee";
    public static final String PAID_TOTAL_FEE = "paidTotalFee";
    public static final String SUCCESS = "success";
    public static final String ERROR_MESSAGE = "errorMessage";
    public static final String POSITION_IN_BLOCK = "positionInBlock";
    public static final String WRAPPED = "wrapped";
    public static final String POSITION_IN_WRAPPED_TXN = "positionInWrappedTxn";
    public static final String SIZE = "size";

    public static final String BLOCK_NUMBER = "blockNumber";
    public static final String TIME_STAMP = "timeStamp";
    public static final String BLOCK_HASH = "blockHash";
    public static final String ROOT_HASH = "rootHash";
    public static final String PROPOSER = "proposer";
    public static final String PREVIOUS_BLOCK_HASH = "previousBlockHash";
    public static final String BLOCKCHAIN_VERSION = "blockchainVersion";
    public static final String BLOCK_REWARD = "blockReward";
    public static final String BLOCK_SIZE = "blockSize";

    public static final String TITLE = "title";
    public static final String DESCRIPTION = "description";
    public static final String PROPOSAL_HASH = "proposalHash";
    public static final String EARLY_WITHDRAW_TIME = "earlyWithdrawTime";
    public static final String EARLY_WITHDRAW_PENALTY = "earlyWithdrawPenalty";
    public static final String MAX_BLOCK_SIZE = "maxBlockSize";
    public static final String MAX_TRANSACTION_SIZE = "maxTransactionSize";
    public static final String OVERALL_BURN_PERCENTAGE = "overallBurnPercentage";
    public static final String REWARD_PER_YEAR = "rewardPerYear";
    public static final String VALIDATOR_COUNT_LIMIT = "validatorCountLimit";
    public static final String VALIDATOR_JOINING_FEE = "validatorJoiningFee";
    public static final String VIDA_ID_CLAIMING_FEE = "vidaIdClaimingFee";
    public static final String VM_OWNER_TRANSACTION_FEE_SHARE = "vmOwnerTransactionFeeShare";
    public static final String VOTE = "vote";
    public static final String TRANSACTIONS = "transactions";
    public static final String GUARDIAN_ADDRESS = "guardianAddress";
    public static final String GUARDIAN_EXPIRY_DATE = "guardianExpiryDate";
    public static final String IP = "ip";
    public static final String VALIDATOR_ADDRESS = "validatorAddress";
    public static final String FROM_VALIDATOR_ADDRESS = "fromValidatorAddress";
    public static final String TO_VALIDATOR_ADDRESS = "toValidatorAddress";
    public static final String SHARES_AMOUNT = "sharesAmount";
    public static final String ADDRESSES = "addresses";
    public static final String CONDUITS = "conduits";
    public static final String CONDUIT_THRESHOLD = "conduitThreshold";
    public static final String MODE = "mode";
    public static final String VIDA_CONDUIT_POWERS = "vidaConduitPowers";
    public static final String IS_PRIVATE = "isPrivate";
    public static final String PUBLIC_KEY = "publicKey";
    public static final String WITHDRAWN_PWR = "withdrawnPWR";

    //endregion

    static {
        addKey(SENDER);
        addKey(RECEIVER);
        addKey(TRANSACTION_HASH);
        addKey(NONCE);
        addKey(AMOUNT);
        addKey(VIDA_ID);
        addKey(DATA);
        addKey(FEE_PER_BYTE);
        addKey(PAID_ACTION_FEE);
        addKey(PAID_TOTAL_FEE);
        addKey(SUCCESS);
        addKey(ERROR_MESSAGE);
        addKey(POSITION_IN_BLOCK);
        addKey(WRAPPED);
        addKey(POSITION_IN_WRAPPED_TXN);
        addKey(SIZE);


        addKey(BLOCK_NUMBER);
        addKey(TIME_STAMP);
        addKey(BLOCK_HASH);
        addKey(ROOT_HASH);
        addKey(PROPOSER);
        addKey(PREVIOUS_BLOCK_HASH);
        addKey(BLOCKCHAIN_VERSION);
        addKey(BLOCK_REWARD);

        addKey(TITLE);
        addKey(DESCRIPTION);
        addKey(PROPOSAL_HASH);
        addKey(EARLY_WITHDRAW_TIME);
        addKey(EARLY_WITHDRAW_PENALTY);
        addKey(MAX_BLOCK_SIZE);
        addKey(MAX_TRANSACTION_SIZE);
        addKey(OVERALL_BURN_PERCENTAGE);
        addKey(REWARD_PER_YEAR);
        addKey(VALIDATOR_COUNT_LIMIT);
        addKey(VALIDATOR_JOINING_FEE);
        addKey(VIDA_ID_CLAIMING_FEE);
        addKey(VM_OWNER_TRANSACTION_FEE_SHARE);
        addKey(VOTE);
        addKey(TRANSACTIONS);
        addKey(GUARDIAN_ADDRESS);
        addKey(GUARDIAN_EXPIRY_DATE);
        addKey(IP);
        addKey(VALIDATOR_ADDRESS);
        addKey(FROM_VALIDATOR_ADDRESS);
        addKey(TO_VALIDATOR_ADDRESS);
        addKey(SHARES_AMOUNT);
        addKey(ADDRESSES);
        addKey(CONDUITS);
        addKey(CONDUIT_THRESHOLD);
        addKey(MODE);
        addKey(VIDA_CONDUIT_POWERS);
        addKey(IS_PRIVATE);
        addKey(PUBLIC_KEY);
        addKey(WITHDRAWN_PWR);
    }
}

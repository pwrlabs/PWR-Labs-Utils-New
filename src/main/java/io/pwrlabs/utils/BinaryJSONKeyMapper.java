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

package io.pwrlabs.util.encoders;

import io.pwrlabs.hashing.PWRHash;

import java.math.BigInteger;
import java.util.Arrays;

public record ByteArrayWrapper(byte[] data) {

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public ByteArrayWrapper {
        //ObjectsInMemoryTracker.trackObject(this);
        if (data == null) {
            throw new NullPointerException();
        }
    }

    public String toHexString() {

        char[] hexChars = new char[data.length * 2];
        for (int i = 0, j = 0; i < data.length; i++) {

            int v = data[i] & 0xFF;
            hexChars[j++] = HEX_ARRAY[v >>> 4];
            hexChars[j++] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public String toChecksumAddress() {

        String hexAddress = toHexString().toLowerCase();
        String addressHash = Hex.toHexString(PWRHash.hash256(hexAddress.getBytes())); // Placeholder for Keccak-256 hash function

        StringBuilder result = new StringBuilder(hexAddress.length());
        for (int i = 0; i < hexAddress.length(); i++) {

            if (Integer.parseInt(String.valueOf(addressHash.charAt(i)), 16) >= 8) {

                result.append(Character.toUpperCase(hexAddress.charAt(i)));
            } else {
                result.append(hexAddress.charAt(i));
            }
        }

        return "0x" + result.toString();
    }

    public BigInteger toBigInteger() {
        return new BigInteger(1, data);
    }

    @Override
    public boolean equals(Object other) {

        if (!(other instanceof ByteArrayWrapper)) {
            return false;
        }
        return Arrays.equals(data, ((ByteArrayWrapper) other).data);
    }

    @Override
    public int hashCode() {

        return Arrays.hashCode(data);
    }

}



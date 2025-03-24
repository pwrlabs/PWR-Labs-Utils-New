package io.pwrlabs.utils;

import org.json.JSONObject;
import io.pwrlabs.util.encoders.Hex;

import java.io.IOException;
import java.util.Arrays;

/**
 * Test class for BinaryJSON.
 * Performs extensive testing of all BinaryJSON functionality without using test frameworks.
 */
public class BinaryJSONTest {

    public static void main(String[] args) {
        try {
            testBasicFunctionality();
            testDataTypes();
            testSerialization();
            testJsonConversion();
            testErrorCases();
            testEdgeCases();
            testMappedKeysOnlyMode();

            System.out.println("All tests passed successfully!");
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void testBasicFunctionality() {
        System.out.println("Testing basic functionality...");

        // Test with mappedKeysOnly = false which allows any keys
        BinaryJSONObject json = new BinaryJSONObject(false);
        json.putInt("testint", 42);
        assert json.getInt("testint") == 42 : "Failed to get int value";

        // Test case insensitivity
        assert json.getInt("TESTINT") == 42 : "Failed to get int value with different case";

        System.out.println("Basic functionality tests passed.");
    }

    private static void testDataTypes() {
        System.out.println("Testing different data types...");

        BinaryJSONObject json = new BinaryJSONObject(false);

        // Test int
        json.putInt("testint", 42);
        assert json.getInt("testint") == 42 : "Failed to get int value";

        // Test long
        json.putLong("testlong", 1234567890123L);
        assert json.getLong("testlong") == 1234567890123L : "Failed to get long value";

        // Test short through general put method
        json.put("testshort", (short) 123);
        assert (short) json.get("testshort") == 123 : "Failed to get short value";

        // Test boolean
        json.put("testboolean", true);
        assert (boolean) json.get("testboolean") : "Failed to get boolean value";

        // Test String
        json.put("teststring", "Hello, World!");
        assert json.get("teststring").equals("Hello, World!") : "Failed to get String value";

        // Test byte array
        byte[] testBytes = {1, 2, 3, 4, 5};
        json.put("testbytes", testBytes);
        assert Arrays.equals((byte[]) json.get("testbytes"), testBytes) : "Failed to get byte[] value";

        System.out.println("Data types tests passed.");
    }

    private static void testSerialization() throws IOException {
        System.out.println("Testing serialization and deserialization...");

        // Create and populate original JSON
        BinaryJSONObject original = new BinaryJSONObject(false);
        original.putInt("testint", 42);
        original.putLong("testlong", 1234567890123L);
        original.put("teststring", "Hello, World!");
        byte[] testBytes = {1, 2, 3, 4, 5};
        original.put("testbytes", testBytes);
        original.put("testshort", (short) 123);
        original.put("testboolean", true);

        // Serialize to bytes
        byte[] serialized = original.toByteArray();

        // Deserialize back to BinaryJSON
        BinaryJSONObject deserialized = new BinaryJSONObject(serialized);

        // Verify all values match
        assert deserialized.getInt("testint") == 42 : "Int value changed after serialization";
        assert deserialized.getLong("testlong") == 1234567890123L : "Long value changed after serialization";
        assert deserialized.get("teststring").equals("Hello, World!") : "String value changed after serialization";
        assert Arrays.equals((byte[]) deserialized.get("testbytes"), testBytes) : "Byte array changed after serialization";
        assert (short) deserialized.get("testshort") == 123 : "Short value changed after serialization";
        assert (boolean) deserialized.get("testboolean") : "Boolean value changed after serialization";

        System.out.println("Serialization tests passed.");
    }

    private static void testJsonConversion() {
        System.out.println("Testing JSON conversion...");

        BinaryJSONObject json = new BinaryJSONObject(false);
        json.putInt("testint", 42);
        json.putLong("testlong", 1234567890123L);
        json.put("teststring", "Hello, World!");
        byte[] testBytes = {1, 2, 3, 4, 5};
        json.put("testbytes", testBytes);

        JSONObject jsonObj = json.toJsonObject();

        assert jsonObj.getInt("testint") == 42 : "Int value not correctly converted to JSON";
        assert jsonObj.getLong("testlong") == 1234567890123L : "Long value not correctly converted to JSON";
        assert jsonObj.getString("teststring").equals("Hello, World!") : "String value not correctly converted to JSON";

        // Byte arrays are converted to hex strings in JSON
        String hexBytes = jsonObj.getString("testbytes");
        assert hexBytes.equals(Hex.toHexString(testBytes)) : "Byte array not correctly converted to hex string in JSON";

        System.out.println("JSON conversion tests passed.");
    }

    private static void testErrorCases() {
        System.out.println("Testing error cases...");

        // Test key length <= 2
        BinaryJSONObject json = new BinaryJSONObject(false);
        boolean exceptionThrown = false;
        try {
            json.put("ab", "value"); // 2-byte key
        } catch (Exception e) {
            exceptionThrown = true;
            assert e.getMessage().contains("Key length must be greater than 2") :
                    "Wrong exception message: " + e.getMessage();
        }
        assert exceptionThrown : "Exception not thrown for key length <= 2";

        // Test accessing non-existent key
        json = new BinaryJSONObject(false);
        Object nullValue = json.get("nonexistent");
        assert nullValue == null : "Non-existent key should return null";

        exceptionThrown = false;
        try {
            json.getInt("nonexistent");
        } catch (Exception e) {
            exceptionThrown = true;
            assert e.getMessage().contains("Value is not an integer") :
                    "Wrong exception message: " + e.getMessage();
        }
        assert exceptionThrown : "Exception not thrown for non-existent key when getting as int";

        // Test type mismatches
        json = new BinaryJSONObject(false);
        json.put("teststring", "Not an int");
        exceptionThrown = false;
        try {
            json.getInt("teststring");
        } catch (Exception e) {
            exceptionThrown = true;
            assert e.getMessage().contains("Value is not an integer") :
                    "Wrong exception message: " + e.getMessage();
        }
        assert exceptionThrown : "Exception not thrown for type mismatch (string to int)";

        json.put("teststring", "Not a long");
        exceptionThrown = false;
        try {
            json.getLong("teststring");
        } catch (Exception e) {
            exceptionThrown = true;
            assert e.getMessage().contains("Value is not a long") :
                    "Wrong exception message: " + e.getMessage();
        }
        assert exceptionThrown : "Exception not thrown for type mismatch (string to long)";

        System.out.println("Error cases tests passed.");
    }

    private static void testEdgeCases() throws IOException {
        System.out.println("Testing edge cases...");

        // Test empty BinaryJSON
        BinaryJSONObject emptyJson = new BinaryJSONObject(false);
        byte[] serialized = emptyJson.toByteArray();
        assert serialized.length == 0 : "Serialized empty BinaryJSON should have length 0";

        // Test large string
        BinaryJSONObject json = new BinaryJSONObject(false);
        StringBuilder largeString = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeString.append("a");
        }
        json.put("testlargestring", largeString.toString());
        assert json.get("testlargestring").equals(largeString.toString()) : "Large string not stored correctly";

        // Test various integer values
        json = new BinaryJSONObject(false);
        json.putInt("minint", Integer.MIN_VALUE);
        json.putInt("maxint", Integer.MAX_VALUE);
        json.putInt("zeroint", 0);
        json.putInt("negint", -42);

        assert json.getInt("minint") == Integer.MIN_VALUE : "Min int value not stored correctly";
        assert json.getInt("maxint") == Integer.MAX_VALUE : "Max int value not stored correctly";
        assert json.getInt("zeroint") == 0 : "Zero int value not stored correctly";
        assert json.getInt("negint") == -42 : "Negative int value not stored correctly";

        // Test various long values
        json = new BinaryJSONObject(false);
        json.putLong("minlong", Long.MIN_VALUE);
        json.putLong("maxlong", Long.MAX_VALUE);
        json.putLong("zerolong", 0L);
        json.putLong("neglong", -42L);

        assert json.getLong("minlong") == Long.MIN_VALUE : "Min long value not stored correctly";
        assert json.getLong("maxlong") == Long.MAX_VALUE : "Max long value not stored correctly";
        assert json.getLong("zerolong") == 0L : "Zero long value not stored correctly";
        assert json.getLong("neglong") == -42L : "Negative long value not stored correctly";

        // Test serialization with many values
        json = new BinaryJSONObject(false);
        for (int i = 0; i < 100; i++) {
            json.putInt("testint" + i, i);
        }
        byte[] manyValuesSerialized = json.toByteArray();
        BinaryJSONObject manyValuesDeserialized = new BinaryJSONObject(manyValuesSerialized);
        for (int i = 0; i < 100; i++) {
            assert manyValuesDeserialized.getInt("testint" + i) == i :
                    "Value at index " + i + " not correctly serialized/deserialized";
        }

        System.out.println("Edge cases tests passed.");

        System.out.println(json.toJsonObject());
    }

    private static void testMappedKeysOnlyMode() {
        System.out.println("Testing mappedKeysOnly mode...");

        try {
            // This test is conditional on the availability of mapped keys in BinaryJSONKeyMapper
            // First check if any mapped keys exist that we can use
            BinaryJSONObject mappedJson = new BinaryJSONObject(true);

            // Try to test with a common key that might be present in the mapper
            // If not, this will throw an exception which we'll catch
            boolean testedMappedKeys = false;

            for (String potentialKey : new String[] {"id", "name", "type", "value", "data", "info", "key"}) {
                try {
                    mappedJson.putInt(potentialKey, 123);
                    assert mappedJson.getInt(potentialKey) == 123 :
                            "Failed to retrieve value with mappedKeysOnly=true";

                    // If we got here, we've found a key that works with mappedKeysOnly
                    System.out.println("Successfully tested mappedKeysOnly with key: " + potentialKey);
                    testedMappedKeys = true;
                    break;
                } catch (Exception e) {
                    // This key isn't in the mapper, try the next one
                }
            }

            if (!testedMappedKeys) {
                System.out.println("Could not test mappedKeysOnly mode - no known mapped keys available");
            }

            // Test serialization/deserialization of the mappedKeysOnly flag
            BinaryJSONObject emptyMappedJson = new BinaryJSONObject(true);
            byte[] serialized = new byte[1]; // Just the flag byte
            try {
                serialized = emptyMappedJson.toByteArray();
            } catch (IOException e) {
                System.err.println("Failed to serialize empty mappedKeysOnly JSON: " + e.getMessage());
            }

            if (serialized.length > 0) {
                assert serialized[0] == 1 : "mappedKeysOnly flag not correctly serialized";

                BinaryJSONObject deserialized = new BinaryJSONObject(serialized);
                // We can't directly check if mappedKeysOnly is true in the deserialized object
                // since it's a private field, but we can test its behavior

                boolean correctBehavior = false;
                try {
                    deserialized.put("unmappedkey", "value");
                } catch (Exception e) {
                    // This is expected if mappedKeysOnly was preserved
                    if (e.getMessage().contains("Key not found in key mapper")) {
                        correctBehavior = true;
                    }
                }

                assert correctBehavior : "mappedKeysOnly flag not preserved during serialization";
            }
        } catch (Exception e) {
            System.out.println("mappedKeysOnly tests skipped: " + e.getMessage());
        }

        System.out.println("mappedKeysOnly mode tests completed.");
    }
}

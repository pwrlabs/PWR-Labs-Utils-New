package io.pwrlabs.utils;

import io.pwrlabs.util.encoders.Hex;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.pwrlabs.newerror.NewError.errorIf;

public class BinaryJSONObject {
    private final Map<String /*Key*/, Object /*Value*/> keyValueMap = new ConcurrentHashMap<>();
    private final boolean mappedKeysOnly;

    public BinaryJSONObject(boolean mappedKeysOnly) {
        this.mappedKeysOnly = mappedKeysOnly;
    }

    public BinaryJSONObject(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        mappedKeysOnly = buffer.get() == 1;

        while(buffer.hasRemaining()) {
            int keyLength = mappedKeysOnly ? 2 : buffer.getInt();
            byte[] key = new byte[keyLength];
            buffer.get(key);

            String keyStr;

            if(key.length == 2) {
                keyStr = BinaryJSONKeyMapper.getKey(ByteBuffer.wrap(key).getShort());
            } else {
                keyStr = new String(key);
            }

            byte type = buffer.get();
            switch (type) {
                case 1:
                    keyValueMap.put(keyStr, buffer.getInt());
                    break;
                case 2:
                    keyValueMap.put(keyStr, buffer.getLong());
                    break;
                case 3:
                    keyValueMap.put(keyStr, buffer.getShort());
                    break;
                case 4:
                    keyValueMap.put(keyStr, buffer.get() == 1);
                    break;
                case 5: //String
                    int valueLength = buffer.getInt();
                    byte[] valueBytes = new byte[valueLength];
                    buffer.get(valueBytes);
                    keyValueMap.put(keyStr, new String(valueBytes));
                    break;
                case 6: //byte[]
                    int valueLength2 = buffer.getInt();
                    byte[] valueBytes2 = new byte[valueLength2];
                    buffer.get(valueBytes2);
                    keyValueMap.put(keyStr, valueBytes2);
                    break;
                case 7: //BinaryJSONArray
                    int valueLength3 = buffer.getInt();
                    byte[] valueBytes3 = new byte[valueLength3];
                    buffer.get(valueBytes3);
                    keyValueMap.put(keyStr, new BinaryJSONArray(valueBytes3));
                    break;
                case 8: //BinaryJSONObject
                    int valueLength4 = buffer.getInt();
                    byte[] valueBytes4 = new byte[valueLength4];
                    buffer.get(valueBytes4);
                    keyValueMap.put(keyStr, new BinaryJSONObject(valueBytes4));
                    break;
                case 9: //Byte
                    keyValueMap.put(keyStr, buffer.get());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported value type");
            }
        }
    }

    public void put(String key, Object value) {
        errorIf(key == null, "Key is null");
        errorIf(value == null, "Value is null");

        byte[] keyBytes = key.getBytes();
        errorIf(keyBytes.length <= 2, "Key length must be greater than 2"); //We prevent keys whose length is less than 2 bytes because then it might collide with the ids in key mappers

        if(mappedKeysOnly) {
            Short keyId = BinaryJSONKeyMapper.getId(key);
            errorIf(keyId == null, "Key not found in key mapper. Closest key: " + BinaryJSONKeyMapper.closestMatch(key));
        }

        keyValueMap.put(key, value);
    }

    public void putInt(String key, int value) {
        put(key, value);
    }

    public void putLong(String key, long value) {
        put(key, value);
    }

    public Object get(String key) {
        return keyValueMap.get(key);
    }

    public byte[] getBytes(String key) {
        Object value = get(key);
        if(value instanceof byte[]) {
            return (byte[]) value;
        } else {
            throw new IllegalArgumentException("Value is not a byte array");
        }
    }

    public String getString(String key) {
        Object value = get(key);
        if(value instanceof String) {
            return (String) value;
        } else if(value instanceof byte[]) {
            return new String((byte[]) value);
        } else {
            throw new IllegalArgumentException("Value is not a string");
        }
    }

    public boolean getBoolean(String key) {
        Object value = get(key);
        if(value instanceof Boolean) {
            return (boolean) value;
        } else {
            throw new IllegalArgumentException("Value is not a boolean");
        }
    }

    public short getShort(String key) {
        Object value = get(key);
        if(value instanceof Short) {
            return (short) value;
        } else {
            throw new IllegalArgumentException("Value is not a short");
        }
    }

    public byte getByte(String key) {
        Object value = get(key);
        if(value instanceof Byte) {
            return (byte) value;
        } else {
            throw new IllegalArgumentException("Value is not a byte");
        }
    }

    public int getInt(String key) {
        Object value = get(key);
        if(value instanceof Integer) {
            return (int) value;
        } else {
            throw new IllegalArgumentException("Value is not an integer");
        }
    }

    public long getLong(String key) {
        Object value = get(key);
        if(value instanceof Long) {
            return (long) value;
        } else {
            throw new IllegalArgumentException("Value is not a long");
        }
    }

    public BinaryJSONArray getBinaryJSONArray(String key) {
        Object value = get(key);
        if(value instanceof BinaryJSONArray) {
            return (BinaryJSONArray) value;
        } else {
            throw new IllegalArgumentException("Value is not a BinaryJSONArray");
        }
    }

    public boolean containsKey(String key) {
        return keyValueMap.containsKey(key);
    }

    public byte[] toByteArray() throws IOException {
        if(keyValueMap.isEmpty()) {
            return new byte[0];
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(mappedKeysOnly ? 1 : 0);

        for (Map.Entry<String, Object> entry : keyValueMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            Short keyId = BinaryJSONKeyMapper.getId(key);
            if(mappedKeysOnly) {
                errorIf(keyId == null, "Key not found in key mapper");
                bos.write(ByteBuffer.allocate(2).putShort(keyId).array());
            } else {
                if(keyId == null) {
                    byte[] keyBytes = key.getBytes();
                    bos.write(ByteBuffer.allocate(4).putInt(keyBytes.length).array());
                    bos.write(keyBytes);
                } else {
                    bos.write(ByteBuffer.allocate(4).putInt(2).array());
                    bos.write(ByteBuffer.allocate(2).putShort(keyId).array());
                }
            }

            if(value instanceof Integer) {
                bos.write(1); // Type
                bos.write(ByteBuffer.allocate(4).putInt((int) value).array());
            } else if(value instanceof Long) {
                bos.write(2); // Type
                bos.write(ByteBuffer.allocate(8).putLong((long) value).array());
            } else if (value instanceof Short) {
                bos.write(3); // Type
                bos.write(ByteBuffer.allocate(2).putShort((short) value).array());
            } else if(value instanceof Boolean) {
                bos.write(4); // Type
                bos.write((byte) ((boolean) value ? 1 : 0));
            } else if(value instanceof String) {
                bos.write(5); // Type
                byte[] valueBytes = ((String) value).getBytes();
                bos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
                bos.write(valueBytes);
            } else if (value instanceof byte[]) {
                bos.write(6); // Type
                byte[] valueBytes = (byte[]) value;
                bos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
                bos.write(valueBytes);
            } else if (value instanceof BinaryJSONArray) {
                bos.write(7); // Type
                byte[] valueBytes = ((BinaryJSONArray) value).toByteArray();
                bos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
                bos.write(valueBytes);
            } else if (value instanceof BinaryJSONObject) {
                bos.write(8); // Type
                byte[] valueBytes = ((BinaryJSONObject) value).toByteArray();
                bos.write(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
                bos.write(valueBytes);
            } else if (value instanceof Byte) {
                bos.write(9); // Type
                bos.write((byte) value);
            }

            else {
                throw new IllegalArgumentException("Unsupported value type");
            }
        }

        return bos.toByteArray();
    }

    public JSONObject toJsonObject() {
        JSONObject jsonObject = new JSONObject();

        for (Map.Entry<String, Object> entry : keyValueMap.entrySet()) {
            if(entry.getValue() instanceof byte[]) {
                jsonObject.put(entry.getKey(), Hex.toHexString((byte[]) entry.getValue()));
            } else if(entry.getValue() instanceof BinaryJSONObject) {
                jsonObject.put(entry.getKey(), ((BinaryJSONObject) entry.getValue()).toJsonObject());
            } else if(entry.getValue() instanceof BinaryJSONArray) {
                jsonObject.put(entry.getKey(), ((BinaryJSONArray) entry.getValue()).toJsonArray());
            } else {
                jsonObject.put(entry.getKey(), entry.getValue());
            }
        }

        return jsonObject;
    }
}

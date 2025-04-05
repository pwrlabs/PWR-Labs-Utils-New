package io.pwrlabs.utils;

import io.pwrlabs.concurrency.ConcurrentList;
import io.pwrlabs.util.encoders.Hex;
import org.json.JSONArray;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BinaryJSONArray {
    private final ConcurrentList<Object> values = new ConcurrentList<>();

    public BinaryJSONArray() {
    }

    public BinaryJSONArray(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        while(buffer.hasRemaining()) {
            byte type = buffer.get();
            switch (type) {
                case 1:
                    values.add(buffer.getInt());
                    break;
                case 2:
                    values.add(buffer.getLong());
                    break;
                case 3:
                    values.add(buffer.getShort());
                    break;
                case 4:
                    values.add(buffer.get() == 1);
                    break;
                case 5: //String
                    int valueLength = buffer.getInt();
                    byte[] valueBytes = new byte[valueLength];
                    buffer.get(valueBytes);
                    values.add(new String(valueBytes));
                    break;
                case 6: //byte[]
                    int valueLength2 = buffer.getInt();
                    byte[] valueBytes2 = new byte[valueLength2];
                    buffer.get(valueBytes2);
                    values.add(valueBytes2);
                    break;
                case 7: //BinaryJSONArray
                    int valueLength3 = buffer.getInt();
                    byte[] valueBytes3 = new byte[valueLength3];
                    buffer.get(valueBytes3);
                    values.add(new BinaryJSONArray(valueBytes3));
                    break;
                case 8: //BinaryJSONObject
                    int valueLength4 = buffer.getInt();
                    byte[] valueBytes4 = new byte[valueLength4];
                    buffer.get(valueBytes4);
                    values.add(new BinaryJSONObject(valueBytes4));
                    break;
                case 9: //Byte
                    values.add(buffer.get());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported value type");
            }
        }
    }

    public void add(Object value) {
        values.add(value);
    }

    public ConcurrentList<Object> getValues() {
        return values;
    }

    public int size() {
        return values.size();
    }

    public Object get(int index) {
        return values.get(index);
    }

    public byte[] toByteArray() throws IOException {
        if(values.size() == 0) {
            return new byte[0];
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        for(Object value: values.getArrayListCopy()) {
            if (value instanceof Integer) {
                bos.write(1); // Type
                bos.write(ByteBuffer.allocate(4).putInt((int) value).array());
            } else if (value instanceof Long) {
                bos.write(2); // Type
                bos.write(ByteBuffer.allocate(8).putLong((long) value).array());
            } else if (value instanceof Short) {
                bos.write(3); // Type
                bos.write(ByteBuffer.allocate(2).putShort((short) value).array());
            } else if (value instanceof Boolean) {
                bos.write(4); // Type
                bos.write((byte) ((boolean) value ? 1 : 0));
            } else if (value instanceof String) {
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
            } else {
                throw new IllegalArgumentException("Unsupported value type");
            }
        }

        return bos.toByteArray();
    }

    public JSONArray toJsonArray() {
        JSONArray jsonArray = new JSONArray();
        for(Object value: values.getArrayListCopy()) {
            if(value instanceof byte[]) {
                jsonArray.put(Hex.toHexString((byte[]) value));
            } else if (value instanceof BinaryJSONObject) {
                jsonArray.put(((BinaryJSONObject) value).toJsonObject());
            } else if (value instanceof BinaryJSONArray) {
                jsonArray.put(((BinaryJSONArray) value).toJsonArray());
            } else {
                jsonArray.put(value);
            }
        }

        return jsonArray;
    }
}

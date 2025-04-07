package io.pwrlabs.util.encoders;

public class BiResult<T, V> {
    private final T first;
    private final V second;

    public BiResult(T first, V second) {
        this.first = first;
        this.second = second;
    }

    public T getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        BiResult<?, ?> BiResult = (BiResult<?, ?>) obj;

        if (first != null ? !first.equals(BiResult.first) : BiResult.first != null) return false;
        return second != null ? second.equals(BiResult.second) : BiResult.second == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }
}
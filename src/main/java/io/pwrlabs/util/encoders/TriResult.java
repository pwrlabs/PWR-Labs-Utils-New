package io.pwrlabs.util.encoders;

public class TriResult<T, V, Z> {
    private final T first;
    private final V second;
    private final Z third;

    public TriResult(T first, V second, Z third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public T getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }

    public Z getThird() {
        return third;
    }

    @Override
    public String toString() {
        return "TriResult{" +
                "first=" + first +
                ", second=" + second +
                ", third=" + third +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        TriResult<?, ?, ?> that = (TriResult<?, ?, ?>) obj;
        if (first != null ? !first.equals(that.first) : that.first != null) return false;
        if (second != null ? !second.equals(that.second) : that.second != null) return false;
        return third != null ? third.equals(that.third) : that.third == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        result = 31 * result + (third != null ? third.hashCode() : 0);
        return result;
    }
}

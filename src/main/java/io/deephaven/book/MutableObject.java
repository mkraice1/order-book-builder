package io.deephaven.book;

public final class MutableObject<T> {
    private T value = null;

    public void set(T val) {
        this.value = val;
    }

    public T get() {
        return value;
    }
}

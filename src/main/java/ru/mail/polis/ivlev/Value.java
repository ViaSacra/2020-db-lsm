package ru.mail.polis.ivlev;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

import static ru.mail.polis.ivlev.Time.getCurrentTime;

public final class Value implements Comparable<Value> {

    private final long timestamp;
    private final ByteBuffer data;

    Value(final long timestamp, final ByteBuffer data) {
        this.timestamp = timestamp;
        this.data = data;
    }

    Value(final long timestamp) {
        this.timestamp = timestamp;
        this.data = null;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(Time.getCurrentTime(), data.duplicate());
    }

    public static Value tombstone() {
        return new Value(getCurrentTime(), null);
    }

    @NotNull
    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("Removed");
        }
        return data.asReadOnlyBuffer();
    }

    public boolean isRemoved() {
        return data == null;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public long getTimeStamp() {
        return timestamp;
    }
}

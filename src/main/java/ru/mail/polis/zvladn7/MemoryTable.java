package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryTable implements Table {

    private final static int LONG_BYTES = 8;

    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();

    private final long amountOfBytesToFlush;
    private long currentAmountOfBytes;

    public MemoryTable(final long amountOfBytesToFlush) {
        this.amountOfBytesToFlush = amountOfBytesToFlush;
        this.currentAmountOfBytes = 0;
    }

    public boolean isFull() {
        return currentAmountOfBytes >= amountOfBytesToFlush;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(entry -> new Cell(entry.getKey(), entry.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        map.put(key, new Value(System.currentTimeMillis(), value));
        currentAmountOfBytes = key.remaining() + value.remaining() + LONG_BYTES;
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        Value value = map.get(key);
        if (value != null && !value.isTombstone()) {
            currentAmountOfBytes -= value.getData().remaining();
        } else {
            currentAmountOfBytes += key.remaining() + LONG_BYTES;
        }

        map.put(key, new Value(System.currentTimeMillis()));
    }

    @Override
    public int size() {
        return map.size();
    }
}

package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryTable implements Table {

    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();

    private int currentAmountOfBytes;

    public MemoryTable() {
        this.currentAmountOfBytes = 0;
    }

    public int getSize() {
        return currentAmountOfBytes;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(entry -> new Cell(entry.getKey(), entry.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value val = map.get(key);
        if (val == null) {
            currentAmountOfBytes += key.remaining() + value.remaining() + Long.BYTES;
        } else {
            currentAmountOfBytes += value.remaining() - val.getData().remaining();
        }
        map.put(key.duplicate(), new Value(System.currentTimeMillis(), value.duplicate()));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value value = map.get(key);
        if (value == null) {
            currentAmountOfBytes += key.remaining() + Long.BYTES;
        } else if (!value.isTombstone()) {
            currentAmountOfBytes -= value.getData().remaining();
        }

        map.put(key.duplicate(), new Value(System.currentTimeMillis()));
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void close() {
        map.clear();
        currentAmountOfBytes = 0;
    }
}

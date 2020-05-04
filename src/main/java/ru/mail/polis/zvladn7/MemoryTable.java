package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryTable implements Table {

    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();

//    private final long amountOfBytesToFlush;
    private int currentAmountOfBytes;

    public MemoryTable() {
        this.currentAmountOfBytes = 0;
    }

    public boolean isFull() {
//        return currentAmountOfBytes >= amountOfBytesToFlush;
        throw new UnsupportedOperationException();
    }

    public int getSize() {
        return currentAmountOfBytes;
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
        if (map.containsKey(key)) {
            currentAmountOfBytes += key.remaining() + value.remaining() + Long.BYTES;
        } else {
            currentAmountOfBytes += value.remaining() + Long.BYTES;
        }
        map.put(key, new Value(System.currentTimeMillis(), value));
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        Value value = map.get(key);
        if (value != null && !value.isTombstone()) {
            currentAmountOfBytes -= value.getData().remaining();
        } else {
            currentAmountOfBytes += key.remaining() + Long.BYTES;
        }

        map.put(key, new Value(System.currentTimeMillis()));
    }

    @Override
    public int size() {
        return map.size();
    }


    @Override
    public void close() {
        map.clear();
    }
}

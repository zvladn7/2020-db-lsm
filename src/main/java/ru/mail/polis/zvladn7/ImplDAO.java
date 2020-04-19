package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class ImplDAO implements DAO {

    private final SortedMap<ByteBuffer, ByteBuffer> sortedMap = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final boolean isDropWhile = sortedMap.containsKey(from);
        return sortedMap
                .entrySet()
                .stream()
                .map(entry -> Record.of(entry.getKey(), entry.getValue()))
                .dropWhile(record -> isDropWhile && !record.getKey().equals(from))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        sortedMap.put(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        sortedMap.remove(key);
    }

    @Override
    public void close() throws IOException {
        sortedMap.clear();
    }
}

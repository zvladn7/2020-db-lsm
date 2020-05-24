package ru.mail.polis.zvladn7;

import com.google.common.collect.Iterators;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class LsmDAOImpl implements LsmDAO {

    private static final Logger log = Logger.getLogger(LsmDAOImpl.class.getName());

    private static final String SSTABLE_FILE_POSTFIX = ".dat";
    private static final String SSTABLE_TEMPORARY_FILE_POSTFIX = ".tmp";

    private static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    @NonNull
    private final File storage;
    private final int amountOfBytesToFlush;

    private MemoryTable memtable;
    private final NavigableMap<Integer, Table> ssTables;
    private final HashMap<ByteBuffer, Long> lockTable;

    private int generation;

    /**
     * LSM DAO implementation.
     * @param storage - the directory where SSTables stored.
     * @param amountOfBytesToFlush - amount of bytes that need to flush current memory table.
     */
    public LsmDAOImpl(@NotNull final File storage, final int amountOfBytesToFlush) throws IOException {
        this.storage = storage;
        this.amountOfBytesToFlush = amountOfBytesToFlush;
        this.memtable = new MemoryTable();
        this.ssTables = new TreeMap<>();
        this.lockTable = new HashMap<>();
        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(file -> !file.toFile().isDirectory() && file.toString().endsWith(SSTABLE_FILE_POSTFIX))
                    .forEach(file -> {
                        final String fileName = file.getFileName().toString();
                        try {
                            final String stringGen = fileName.substring(0, fileName.indexOf(SSTABLE_FILE_POSTFIX));
                            final int gen = Integer.parseInt(stringGen);
                            generation = Math.max(gen, generation);
                            ssTables.put(gen, new SSTable(file.toFile()));
                        } catch (IOException e) {
                            e.printStackTrace();
                            log.log(Level.INFO, "Something went wrong while the SSTable was created!", e);
                        } catch (NumberFormatException e) {
                            log.log(Level.INFO, "Unexpected name of SSTable file: " + fileName, e);
                        }
                    });
            ++generation;
        }

    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final Iterator<Cell> freshElements = freshCellIterator(from);
        final Iterator<Cell> aliveElements = Iterators.filter(freshElements, el -> !el.getValue().isTombstone());

        return Iterators.transform(aliveElements, el -> Record.of(el.getKey(), el.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memtable.upsert(key, value);
        if (memtable.getAmountOfBytes() > amountOfBytesToFlush) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memtable.remove(key);
        if (memtable.getAmountOfBytes() > amountOfBytesToFlush) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (memtable.size() > 0) {
            flush();
        }
        ssTables.values().forEach(Table::close);
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Cell> freshElements = freshCellIterator(EMPTY_BUFFER);
        final File dst = serialize(freshElements);

        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(f -> !f.getFileName().toFile().toString().equals(dst.getName()))
                .forEach(f -> {
                    try {
                        Files.delete(f);
                    } catch (IOException e) {
                        log.log(Level.WARNING, "Unable to delete file: " + f.getFileName().toFile().toString(), e);
                    }
                });
        }

        ssTables.clear();
        memtable = new MemoryTable();
        ssTables.put(generation, new SSTable(dst));
        ++generation;
    }

    private void flush() throws IOException {
        final File dst = serialize(memtable.iterator(EMPTY_BUFFER));
        ++generation;
        ssTables.put(generation, new SSTable(dst));
        memtable = new MemoryTable();
    }

    private Iterator<Cell> freshCellIterator(@NotNull final ByteBuffer from) {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        iters.add(memtable.iterator(from));
        ssTables.descendingMap().values().forEach(ssTable -> {
            try {
                iters.add(ssTable.iterator(from));
            } catch (IOException e) {
                log.log(Level.INFO, "Something went wrong when the SSTable iterator was added to list iter!", e);
            }
        });

        final Iterator<Cell> mergedElements = Iterators.mergeSorted(
                iters,
                Cell.BY_KEY_AND_VALUE_CREATION_TIME_COMPARATOR
        );

        return Iters.collapseEquals(mergedElements, Cell::getKey);
    }

    private File serialize(final Iterator<Cell> iterator) throws IOException {
        final File file = new File(storage, generation + SSTABLE_TEMPORARY_FILE_POSTFIX);
        file.createNewFile();
        SSTable.serialize(file, iterator);
        final String newFileName = generation + SSTABLE_FILE_POSTFIX;
        final File dst = new File(storage, newFileName);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        return dst;
    }

    @Override
    public TransactionalDAO beginTransaction() {
        return new TransactionalDAOImpl(storage.getAbsolutePath(), this);
    }

    @Override
    public void lock(@NotNull ByteBuffer key, @NotNull Long id) {
        lockTable.put(key, id);
    }

    @Override
    public boolean isLocked(@NotNull ByteBuffer key, @NotNull Long id) {
        Long lockId = lockTable.get(key);
        if (lockId == null) {
            lock(key, id);
            return false;
        }
        return !id.equals(lockId);
    }
}

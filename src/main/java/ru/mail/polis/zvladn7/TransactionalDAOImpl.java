package ru.mail.polis.zvladn7;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

final class TransactionalDAOImpl implements TransactionalDAO {

    private static final Logger logger = Logger.getLogger(TransactionalDAOImpl.class.getName());
    private static long nextId;

    private final long id;
    private final MemoryTable memoryTable;
    private final LsmDAOImpl dao;

    /**
     * TransactionalDAO implementation.
     * @param dao - DAO which has started transaction
     */
    TransactionalDAOImpl(@NotNull final LsmDAOImpl dao) {
        this.memoryTable = new MemoryTable();
        this.dao = dao;
        this.id = nextId++;
    }

    @Override
    public void commit() {
        memoryTable.iterator(ByteBuffer.allocate(0)).forEachRemaining(cell -> {
            try {
                if (cell.getValue().isTombstone()) {
                    dao.remove(cell.getKey());
                } else {
                    dao.upsert(cell.getKey(), cell.getValue().getData());

                }
            } catch (IOException e) {
                logger.log(Level.WARNING, "The error occurred while transaction was trying to commit, id: " + id, e);
            }
        });
    }

    @Override
    public void rollback() {
        memoryTable.clear();
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final List<Iterator<Cell>> iters = new ArrayList<>();
        iters.add(memoryTable.iterator(from));
        iters.addAll(dao.getAllCellItersList(from));

        final Iterator<Cell> mergedElements = Iterators.mergeSorted(
                iters,
                Cell.BY_KEY_AND_VALUE_CREATION_TIME_COMPARATOR
        );

        final Iterator<Cell> freshElements = Iters.collapseEquals(mergedElements, Cell::getKey);
        final Iterator<Cell> aliveElements = Iterators.filter(freshElements, el -> !el.getValue().isTombstone());

        return Iterators.transform(aliveElements, el -> Record.of(el.getKey(), el.getValue().getData()));
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) {
        lock(key);

        final Iterator<Record> iter = iterator(key);
        if (!iter.hasNext()) {
            throw new NoSuchElementException("Record wasn't found");
        }

        final Record next = iter.next();
        if (next.getKey().equals(key)) {
            return next.getValue();
        } else {
            throw new NoSuchElementException("Record wasn't found");
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        lock(key);
        memoryTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        lock(key);
        memoryTable.remove(key);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("close() aren't supported for TransactionalDAO class");
    }

    private void lock(@NotNull final ByteBuffer key) {
        try {
            dao.lock(key, id);
        } catch (ConcurrentModificationException ex) {
            logger.log(Level.INFO, "Transaction with id: " + id + " was rolled back!");
            rollback();
            throw ex;
        }
    }
}

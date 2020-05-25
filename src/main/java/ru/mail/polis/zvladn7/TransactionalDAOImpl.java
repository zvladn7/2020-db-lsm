package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TransactionalDAOImpl implements TransactionalDAO {

    private static final Logger logger = Logger.getLogger(TransactionalDAOImpl.class.getName());
    private static long nextId;

    private final long id;
    private final MemoryTable memoryTable;
    private final LsmDAO dao;

    /**
     * TransactionalDAO implementation.
     * @param dao - DAO which has started transaction
     */
    public TransactionalDAOImpl(@NotNull final LsmDAO dao) {
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
        //nothing to do now
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        throw new UnsupportedOperationException("Iterator is unsupported for transaction");
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException {
        if (dao.isLocked(key, id)) {
            throw new ConcurrentModificationException("The key has been already locked. get() cannot be performed!");
        }
        ByteBuffer value = memoryTable.get(key);
        if (value == null) {
            value = dao.get(key);
        }
        return value;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        if (dao.isLocked(key, id)) {
            throw new ConcurrentModificationException("The key has been already locked. upsert() cannot be performed!");
        }
        memoryTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (dao.isLocked(key, id)) {
            throw new ConcurrentModificationException("The key has been already locked. remove() cannot be performed!");
        }
        memoryTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        //nothing to close
    }
}

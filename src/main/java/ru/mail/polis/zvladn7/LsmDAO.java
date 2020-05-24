package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;

import java.nio.ByteBuffer;

public interface LsmDAO extends DAO {

    /**
     * Begin new transaction
     */
    TransactionalDAO beginTransaction();

    /**
     * Lock key for other transactions
     */
    void lock(@NotNull ByteBuffer key, @NotNull Long id);

    /**
     * Check if the key is locked for current transaction
     */
    boolean isLocked(@NotNull ByteBuffer key, @NotNull Long id);

}

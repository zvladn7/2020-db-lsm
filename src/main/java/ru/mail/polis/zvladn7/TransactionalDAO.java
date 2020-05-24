package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface TransactionalDAO extends DAO {

    void commit();

    void rollback();

    @NotNull
    ByteBuffer get(@NotNull ByteBuffer key) throws IOException;

}

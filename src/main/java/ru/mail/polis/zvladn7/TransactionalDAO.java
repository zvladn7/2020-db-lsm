package ru.mail.polis.zvladn7;

import ru.mail.polis.DAO;

public interface TransactionalDAO extends DAO {

    void commit();

    void rollback();

}

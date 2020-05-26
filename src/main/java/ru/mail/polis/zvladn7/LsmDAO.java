package ru.mail.polis.zvladn7;

import ru.mail.polis.DAO;

public interface LsmDAO extends DAO {

    /**
     * Begin new transaction.
     */
    TransactionalDAO beginTransaction();

}

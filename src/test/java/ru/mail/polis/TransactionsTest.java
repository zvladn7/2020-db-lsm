package ru.mail.polis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.zvladn7.LsmDAO;
import ru.mail.polis.zvladn7.TransactionalDAO;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransactionsTest extends TestBase {

    @Test
    void lock(@TempDir File data) throws IOException {
        final int amount = 3;

        //create 3 keys and one value
        final List<ByteBuffer> keys = new ArrayList<>(amount);
        final List<ByteBuffer> values = new ArrayList<>(amount);
        for (int i = 0; i < amount; i++) {
            keys.add(randomKey());
            values.add(randomValue());
        }

        try (LsmDAO dao = DAOFactory.create(data)) {
            for (int i = 0; i < amount; ++i) {
                dao.upsert(keys.get(i), values.get(i));
            }

            TransactionalDAO firstTransaction = dao.beginTransaction();
            TransactionalDAO secondTransaction = dao.beginTransaction();

            firstTransaction.upsert(keys.get(0), values.get(1));
            assertThrows(ConcurrentModificationException.class, () -> secondTransaction.remove(keys.get(0)));
            assertThrows(ConcurrentModificationException.class, () -> secondTransaction.get(keys.get(0)));
            assertThrows(ConcurrentModificationException.class, () -> secondTransaction.upsert(keys.get(0), values.get(0)));

            assertEquals(values.get(1), secondTransaction.get(keys.get(1)));
            assertThrows(ConcurrentModificationException.class, () -> firstTransaction.remove(keys.get(1)));
            assertThrows(ConcurrentModificationException.class, () -> firstTransaction.get(keys.get(1)));
            assertThrows(ConcurrentModificationException.class, () -> firstTransaction.upsert(keys.get(1), values.get(0)));

            firstTransaction.rollback();
            secondTransaction.rollback();
        }
    }

    @Test
    void commit(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        try (LsmDAO dao = DAOFactory.create(data)) {
            TransactionalDAO transaction = dao.beginTransaction();

            transaction.upsert(key, value);
            assertEquals(value, transaction.get(key));
            assertThrows(NoSuchElementException.class, () -> dao.get(key));

            transaction.commit();
            assertEquals(value, dao.get(key));
        }
    }

    @Test
    void rollback(@TempDir File data) throws IOException {
        final ByteBuffer key = randomKey();
        final ByteBuffer value = randomValue();

        try (LsmDAO dao = DAOFactory.create(data)) {
            TransactionalDAO transaction = dao.beginTransaction();

            transaction.upsert(key, value);
            assertEquals(value, transaction.get(key));
            assertThrows(NoSuchElementException.class, () -> dao.get(key));

            transaction.rollback();
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        }
    }

    @Test
    void manyRecordsInTransaction(@TempDir File data) throws IOException {
        final int amount = 500;
        final int removeFrom = 200;
        final int removeTo = 300;

        //create 3 keys and one value
        final List<ByteBuffer> keys = new ArrayList<>(amount);
        final List<ByteBuffer> values = new ArrayList<>(amount);
        for (int i = 0; i < amount; i++) {
            keys.add(randomKey());
            values.add(randomValue());
        }

        try (LsmDAO dao = DAOFactory.create(data)) {
            for (int i = 0; i < amount; ++i) {
                dao.upsert(keys.get(i), values.get(i));
            }

            TransactionalDAO transaction = dao.beginTransaction();

            for (int i = 0; i < amount; ++i) {
                transaction.upsert(keys.get(i), values.get(amount - i - 1));

                //check upsert work
                assertEquals(values.get(amount - i - 1), transaction.get(keys.get(i)));
            }

            //check that original data not replaced before commit
            for (int i = 0; i < amount; ++i) {
                assertEquals(values.get(i), dao.get(keys.get(i)));
            }

            //remove values in transaction
            for (int i = removeFrom; i < removeTo; ++i) {
                transaction.remove(keys.get(i));

                final int finalI = i;
                assertThrows(NoSuchElementException.class, () -> transaction.get(keys.get(finalI)));
            }

            //check that original data not replaced before commit
            for (int i = 0; i < amount; ++i) {
                assertEquals(values.get(i), dao.get(keys.get(i)));
            }

            transaction.commit();

            for (int i = 0; i < amount; ++i) {
                if (i >= removeFrom && i < removeTo) {
                    final int finalI = i;
                    assertThrows(NoSuchElementException.class, () -> dao.get(keys.get(finalI)));
                } else {
                    assertEquals(values.get(i), transaction.get(keys.get(amount - i - 1)));
                }
            }


        }
    }

    @Test
    void manyTransactions(@TempDir File data) throws IOException {
        final int amountOfKeyValue = 50;
        final int amountofInsertToDao = 10;
        final int amountOfTransactions = 50;
        final int commitFrom = 5;
        final int commitTo = 45;

        //create 3 keys and one value
        final List<ByteBuffer> keys = new ArrayList<>(amountOfKeyValue);
        final List<ByteBuffer> values = new ArrayList<>(amountOfKeyValue);
        for (int i = 0; i < amountOfKeyValue; i++) {
            keys.add(randomKey());
            values.add(randomValue());
        }

        try (LsmDAO dao = DAOFactory.create(data)) {
            for (int i = 0; i < amountofInsertToDao; ++i) {
                dao.upsert(keys.get(i), values.get(i));
            }

            List<TransactionalDAO> transactions = new ArrayList<>(amountOfTransactions);
            for (int i = 0; i < amountOfTransactions; ++i) {
                transactions.add(dao.beginTransaction());
            }

            for (int i = 0; i < amountofInsertToDao; ++i) {
                transactions.get(i).remove(keys.get(i));

                final int finalI = i;
                assertThrows(NoSuchElementException.class, () -> transactions.get(finalI).get(keys.get(finalI)));
            }

            for (int i = amountofInsertToDao; i < amountOfTransactions; ++i) {
                transactions.get(i).upsert(keys.get(i), values.get(i));

                assertEquals(values.get(i), transactions.get(i).get(keys.get(i)));
            }

            //try to access elements which were used by other transactions
            for (int i = amountofInsertToDao; i < amountOfTransactions; ++i) {
                final int finalI = i;
                assertThrows(ConcurrentModificationException.class,
                        () -> transactions.get(finalI).get(keys.get(amountOfTransactions - finalI - 1)));
            }

            //commit and rollback transactions
            for (int i = 0; i < amountOfTransactions; ++i) {
                if (i >= commitFrom && i <= commitTo) {
                    transactions.get(i).commit();
                } else {
                    transactions.get(i).rollback();
                }
            }

            //check the results of committed transactions and
            //check that roll backed transactions didn't change current dao structure

            //transactions that removed values
            for (int i = 0; i < amountofInsertToDao; ++i) {
                if (i >= commitFrom) {
                    final int finalI = i;
                    assertThrows(NoSuchElementException.class, () -> dao.get(keys.get(finalI)));
                } else {
                    assertEquals(values.get(i), dao.get(keys.get(i)));
                }
            }

            //transactions that insert new values
            for (int i = amountofInsertToDao; i < amountOfKeyValue; ++i) {
                if (i <= commitTo) {
                    assertEquals(values.get(i), dao.get(keys.get(i)));
                } else {
                    final int finalI = i;
                    assertThrows(NoSuchElementException.class, () -> dao.get(keys.get(finalI)));
                }
            }


        }

    }

}

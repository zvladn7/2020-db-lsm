package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

public class SSTable implements Table {

//    private final static Logger log = Logger.getLogger(LSMDAOImpl.class.getName());

    private final int shiftToOffsetsArray;
    private final int amountOfElements;
    private final FileChannel fileChannel;


    /**
     * File structure
     * * [ rows ]
     * * [ rows offset ]
     * * amount of rows
     *
     */
    SSTable(@NotNull final File file) throws IOException {
        fileChannel =  FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final int fileSize = (int) fileChannel.size();

        //get amount
        ByteBuffer offsetBuf = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(offsetBuf, fileSize - Integer.BYTES);
        amountOfElements = offsetBuf.flip().getInt();

        shiftToOffsetsArray = fileSize - Integer.BYTES * (1 + amountOfElements);
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return new SSTableIter(from);
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        throw new UnsupportedEncodingException("SSTable doesn't provide upsert operations!");
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        throw new UnsupportedOperationException("SSTable doesn't provide remove operations!");
    }

    @Override
    public boolean isFull() {
        throw new UnsupportedOperationException("SSTable doesn't provide isFull operations!");
    }

    @Override
    public int size() {
        return amountOfElements;
    }

    static void serialize(final File file, final Iterator<Cell> elementsIter, int amount) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {

//            ByteBuffer offsetsAndAmountBuffer = ByteBuffer.allocate(Integer.BYTES * (amount + 1));
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (elementsIter.hasNext()) {
                final Cell cell = elementsIter.next();
                final ByteBuffer key = cell.getKey();
                final Value value = cell.getValue();
                final int keySize = key.remaining();
                offsets.add(offset);
//                offsetsAndAmountBuffer.putInt(offset);
                offset += keySize + Integer.BYTES * 2 + Long.BYTES;

                //write key size
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                fileChannel.write(buffer.putInt(keySize).flip());
                buffer.clear();

                //write key
                fileChannel.write(key.duplicate());

                //write timestamp
                fileChannel.write(buffer.putLong(value.getTimestamp()).flip());
                buffer.clear();


                if (value.isTombstone()) {
                    fileChannel.write(buffer.putInt(-1).flip());
                } else {
                    final ByteBuffer valueBuffer = value.getData();
                    final int valueSize = valueBuffer.remaining();
                    fileChannel.write(buffer.putInt(valueSize).flip());
                    fileChannel.write(valueBuffer.duplicate());
                    offset += valueSize;
                }
            }

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            for (final int i : offsets) {
                fileChannel.write(buffer.putInt(i).flip());
                buffer.clear();
            }
            fileChannel.write(buffer.putInt(amount).flip());
//            fileChannel.write(offsetsAndAmountBuffer.putInt(amount).rewind());
        }
    }

    private int getOffset(final int position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(buffer,shiftToOffsetsArray + position * Integer.BYTES);
        return buffer.flip().getInt();
    }

    private ByteBuffer getPositionElement(final int position) throws IOException {
        final int keyLengthOffset = getOffset(position);

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(buffer, keyLengthOffset);
        final int keySize = buffer.flip().getInt();

        final ByteBuffer keyBuf = ByteBuffer.allocate(keySize);
        fileChannel.read(keyBuf,keyLengthOffset + Integer.BYTES);

        return keyBuf.flip();
    }

    private int getElementPosition(final ByteBuffer key) throws IOException {
        int left = 0;
        int right = amountOfElements - 1;
        while (left <= right) {
            final int mid = (left + right) / 2;
            final ByteBuffer midKey = getPositionElement(mid);
            final int compareResult = midKey.compareTo(key);

            if (compareResult < 0) {
                left = mid + 1;
            } else if (compareResult > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }

        return left;
    }

    /**
     *
     * Cell(a row of file) structure:
     * key size | key | timestamp | value size | value
     *
     * if value size is 0 than value is absent
     *
     */
    private Cell get(final int position) throws IOException {
        final int elementOffset = getOffset(position);

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(buffer, elementOffset);
        final int keySize = buffer.flip().getInt();
        final int keyOffset = elementOffset + Integer.BYTES;
        buffer.clear();


        final ByteBuffer key = ByteBuffer.allocate(keySize);
        fileChannel.read(key, keyOffset);
        key.flip();

        final int timestampOffset = keyOffset + keySize;
        ByteBuffer timestampBuf = ByteBuffer.allocate(Long.BYTES);
        fileChannel.read(timestampBuf, timestampOffset);
        final long timestamp = timestampBuf.flip().getLong();

        fileChannel.read(buffer, timestampOffset + Long.BYTES);
        final int valueSize = buffer.flip().getInt();

        final Value value;
        if (valueSize == -1) {
            value = new Value(timestamp);
        } else {
            ByteBuffer valueBuf = ByteBuffer.allocate(valueSize);
            fileChannel.read(valueBuf, timestampOffset + Long.BYTES + Integer.BYTES);
            valueBuf.flip();
            value = new Value(timestamp, valueBuf);
        }

        return new Cell(key, value);
    }

    class SSTableIter implements Iterator<Cell> {

        private int position;

        public SSTableIter(final ByteBuffer from) {
            try {
                position = getElementPosition(from.rewind());
            } catch (IOException e) {
//                log.info("SSTable's iterator cannot correctly get 'from' position");
            }
        }

        @Override
        public boolean hasNext() {
            return position < amountOfElements;
        }

        @Override
        public Cell next() {
            try {
                return get(position++);
            } catch (IOException e) {
//                log.info("SSTable's iterator cannot get a cell because it has no more elements");
                throw new NoSuchElementException();
            }
        }
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
        } catch (IOException e) {
//            log.warning("The error was happened when the file channel was closed");
        }
    }
}

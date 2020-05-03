package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SSTable implements Table {

    private static final int INT_BYTES = 4;
    private static final int LONG_BYTES = 8;

    private final int shiftToOffsetsArray;
    private final int amountOfElements;
    private final FileChannel fileChannel;

    /**
     * File structure:
     * * [ rows ]
     * * [ rows offset ]
     * * amount of rows
     *
     */
    SSTable(@NotNull final File file) throws IOException {
         fileChannel =  FileChannel.open(file.toPath(), StandardOpenOption.READ);
         final int fileSize = (int) fileChannel.size();

         final ByteBuffer fileByteBuffer = ByteBuffer.allocate(fileSize);
         fileChannel.read(fileByteBuffer);

         //get amount
//        final int amountOfElementsPosition = fileSize - INT_BYTES;
//        amountOfElements = fileByteBuffer.getInt(amountOfElementsPosition);
        final int amountOfElementsPosition = fileSize - INT_BYTES;
        ByteBuffer offsetBuf = ByteBuffer.allocate(INT_BYTES);
        fileChannel.read(offsetBuf, amountOfElementsPosition);
        amountOfElements = offsetBuf.flip().getInt();


        //get offsets
        shiftToOffsetsArray = fileSize - INT_BYTES * (1 + amountOfElements);
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

            ByteBuffer offsetsAndAmountBuffer = ByteBuffer.allocate(INT_BYTES * (amount + 1));
            int offset = 0;
            while (elementsIter.hasNext()) {
                final Cell cell = elementsIter.next();
                final ByteBuffer key = cell.getKey();
                final Value value = cell.getValue();
                final int keySize = key.remaining();
                offsetsAndAmountBuffer.putInt(offset);
                offset += keySize + INT_BYTES * 2 + LONG_BYTES;

                fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(keySize).rewind());
                fileChannel.write(key.duplicate());
                fileChannel.write(ByteBuffer.allocate(LONG_BYTES).putLong(value.getTimestamp()).rewind());
                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(-1).rewind());
                } else {
                    final ByteBuffer valueBuffer = value.getData();
                    final int valueSize = valueBuffer.remaining();
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(valueSize).rewind());
                    fileChannel.write(valueBuffer.duplicate());
                    offset += valueSize;
                }
            }

            fileChannel.write(offsetsAndAmountBuffer.putInt(amount).rewind());
        }
    }

//    private void fillOffsets(IntBuffer offsetsBuffer) {
//        offsets = new int[amountOfElements];
//        for (int i = 0; i < offsets.length; ++i) {
//            offsets[i] = offsetsBuffer.get(i);
//        }
//    }

    private ByteBuffer getPositionElement(final int position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(INT_BYTES);
        fileChannel.read(buffer,shiftToOffsetsArray + position * INT_BYTES);
        final int keyLengthOffset = buffer.flip().getInt();//offsets[position];
        buffer.clear();

        fileChannel.read(buffer, keyLengthOffset);
        final int keySize = buffer.flip().getInt();

        final int keyOffset = keyLengthOffset + INT_BYTES;
        final ByteBuffer keyBuf = ByteBuffer.allocate(keySize);
        fileChannel.read(keyBuf,keyOffset);

        return keyBuf.flip();
    }

    private int getElementPosition(final ByteBuffer key) throws IOException {
        int left = 0;
        int right = amountOfElements - 1;
        while (left <= right) {
            final int mid = (left + right) / 2;
            final ByteBuffer midKey = getPositionElement(mid);
            final int compareResult = key.compareTo(midKey);

            if (compareResult > 0) {
                left = mid + 1;
            } else if (compareResult < 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }

        return amountOfElements + 1;
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
        final int lengthSizeOffset = shiftToOffsetsArray + position * INT_BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(INT_BYTES);
        fileChannel.read(buffer, lengthSizeOffset);
        final int elementOffset = buffer.flip().getInt();
        buffer.clear();

        fileChannel.read(buffer, elementOffset);
        final int keySize = buffer.flip().getInt();
        final int keyOffset = elementOffset + INT_BYTES;
        buffer.clear();


        final ByteBuffer key = ByteBuffer.allocate(keySize);
        fileChannel.read(key, keyOffset);
        key.flip();

        final int timestampOffset = keyOffset + keySize;
        ByteBuffer timestampBuf = ByteBuffer.allocate(LONG_BYTES);
        fileChannel.read(timestampBuf, timestampOffset);
        final long timestamp = timestampBuf.flip().getLong();
//        final long timestamp = elements.getLong(timestampOffset);

        final int valueSizeOffset = timestampOffset + LONG_BYTES;
        fileChannel.read(buffer, valueSizeOffset);
        final int valueSize = buffer.flip().getInt();
//        final int valueSize = elements.getInt();

        final Value value;
        if (valueSize == -1) {
            value = new Value(timestamp);
        } else {
            final int valueOffset = timestampOffset + LONG_BYTES + INT_BYTES;
            ByteBuffer valueBuf = ByteBuffer.allocate(valueSize);
            fileChannel.read(valueBuf, valueOffset);
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
                e.printStackTrace();
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
                throw new NoSuchElementException();
            }
        }
    }
}

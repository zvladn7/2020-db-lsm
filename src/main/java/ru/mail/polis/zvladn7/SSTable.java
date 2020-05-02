package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTable implements Table {

    private static final int INT_BYTES = 4;
    private static final int LONG_BYTES = 8;

    private final ByteBuffer elements;
    private int[] offsets;

    private final int elementsBufferSize;
    private final int amountOfElements;

    /**
     * File structure:
     * * [ rows ]
     * * [ rows offset ]
     * * amount of rows
     *
     */
    SSTable(@NotNull final File file) throws IOException {
         try (FileChannel fileChannel =  FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
             final int fileSize = (int) fileChannel.size();

             final ByteBuffer fileByteBuffer = ByteBuffer.allocate(fileSize);
             fileChannel.read(fileByteBuffer);

             //get amount
             final int amountOfElementsPosition = fileSize - INT_BYTES;
             amountOfElements = fileByteBuffer.getInt(amountOfElementsPosition);

             //get offsets
             final int offsetsArrayPosition = fileSize - INT_BYTES - INT_BYTES * amountOfElements;
             IntBuffer offsetsBuffer = fileByteBuffer
                     .rewind()
                     .position(offsetsArrayPosition)
                     .limit(amountOfElementsPosition)
                     .slice()
                     .asIntBuffer();

             fillOffsets(offsetsBuffer);


             //get elements
             elements = fileByteBuffer
                     .rewind()
                     .position(0)
                     .limit(offsetsArrayPosition)
                     .slice()
                     .asReadOnlyBuffer();

             elementsBufferSize = offsetsArrayPosition;
         }
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

            while (elementsIter.hasNext()) {
                final Cell cell = elementsIter.next();
                final ByteBuffer key = cell.getKey();
                final Value value = cell.getValue();
                final int keySize = key.remaining();
                int offset = keySize + INT_BYTES * 2 + LONG_BYTES;

                fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(keySize).rewind());
                fileChannel.write(key);
                fileChannel.write(ByteBuffer.allocate(LONG_BYTES).putLong(value.getTimestamp()));
                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(-1).rewind());
                } else {
                    final int valueSize = value.getData().remaining();
                    fileChannel.write(ByteBuffer.allocate(INT_BYTES).putInt(valueSize).rewind());
                    fileChannel.write(value.getData());

                    offset += valueSize;
                }
                offsetsAndAmountBuffer.putInt(offset);
            }
            offsetsAndAmountBuffer.putInt(amount).rewind();

            fileChannel.write(offsetsAndAmountBuffer);
        }
    }

    private void fillOffsets(IntBuffer offsetsBuffer) {
        offsets = new int[amountOfElements];
        for (int i = 0; i < offsets.length; ++i) {
            offsets[i] = offsetsBuffer.get(i);
        }
    }

    private ByteBuffer getPositionElement(final int position) {
        final int keyLengthOffset = offsets[position];
        final int keySize = elements.getInt(keyLengthOffset);
        final int keyOffset = keyLengthOffset + INT_BYTES;
        return elements
                .position(keyOffset)
                .limit(keyOffset + keySize)
                .slice();
    }

    private int getElementPosition(final ByteBuffer key) {
        int left = 0;
        int right = amountOfElements - 1;
        while (left <= right) {
            final int mid = (left + right) / 2;
            final int compareResult = key.compareTo(getPositionElement(mid));

            if (compareResult > 0) {
                left = mid + 1;
            } else if (compareResult < 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }

        return -1;
    }

    /**
     *
     * Cell(a row of file) structure:
     * key size | key | timestamp | value size | value
     *
     * if value size is 0 than value is absent
     *
     */
    private Cell get(final int position) {
        final int elementOffset = offsets[position];

        final int keyOffset = elementOffset + INT_BYTES;
        final int keySize = elements.getInt(elementOffset);

        final int timestampOffset = keyOffset + keySize;


        final ByteBuffer key = elements
                .position(keyOffset)
                .limit(timestampOffset)
                .slice();

        final long timestamp = elements.get(timestampOffset);
        final int valueSize = elements.getInt(timestampOffset + LONG_BYTES);

        final Value value;
        if (valueSize == -1) {
            value = new Value(timestamp);
        } else {
            final int valueOffset = timestampOffset + LONG_BYTES + INT_BYTES;
            final ByteBuffer valyeByteBuffer = elements
                    .position(valueOffset)
                    .limit(valueOffset + valueSize)
                    .slice();
            value = new Value(timestamp, valyeByteBuffer);
        }

        return new Cell(key, value);
    }

    class SSTableIter implements Iterator<Cell> {

        private int position;

        public SSTableIter(final ByteBuffer from) {
            position = getElementPosition(from);
        }

        @Override
        public boolean hasNext() {
            return position < elementsBufferSize;
        }

        @Override
        public Cell next() {
            return get(position);
        }
    }
}

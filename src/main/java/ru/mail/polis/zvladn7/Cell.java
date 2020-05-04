package ru.mail.polis.zvladn7;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cell {

    final static Comparator<Cell> COMPARATOR = Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue);

    private final ByteBuffer key;
    private final Value value;

    public Cell(@NotNull final ByteBuffer key, @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }
}

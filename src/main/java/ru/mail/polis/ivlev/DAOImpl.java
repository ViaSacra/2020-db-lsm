package ru.mail.polis.ivlev;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

public final class DAOImpl implements DAO {

    private static final String SUFFIX = "SSTable.dat";
    private static final String TEMP = "SSTable.tmp";
    private final long flushThreshold;
    private final File file;
    private NavigableMap<Integer, Table> ssTables = new TreeMap<>();
    private MemTable memTable;
    private int generation;

    /**
     * Реализация интерфейса DAO.
     *
     * @param file           - директория
     * @param flushThreshold - максимальный размер таблицы
     */
    public DAOImpl(@NotNull final File file, final long flushThreshold) {
        this.memTable = new MemTable();
        this.flushThreshold = flushThreshold;
        this.file = file;
        this.generation = -1;
        final File[] list = file.listFiles((dir1, name) -> name.endsWith(SUFFIX));
        assert list != null;
        Arrays.stream(list)
                .filter(currentFile -> !currentFile.isDirectory())
                .forEach(f -> {
                            final String name = f.getName();
                            final int gen =
                                    Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                            try {
                                ssTables.put(gen, new SSTable(f));
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            if (gen > generation) {
                                generation = gen;
                            }
                        }
                );
        generation++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Cell> alive = Iterators.filter(compactIterator(from),
                cell -> !requireNonNull(cell).getValue().isRemoved());
        return Iterators.transform(alive, cell ->
                Record.of(requireNonNull(cell).getKey(), cell.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        if (memTable.size() >= flushThreshold) {
            flush();
        }
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (memTable.size() >= flushThreshold) {
            flush();
        }
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }
        ssTables.values().forEach(Table::close);
    }

    @Override
    public void compact() throws IOException {
        final File copyFile = new File(this.file, generation + TEMP);
        SSTable.write(copyFile, compactIterator(Value.EMPTY_BUFFER));
        for (int i = 0; i < generation; i++) {
            Files.delete(new File(this.file, i + SUFFIX).toPath());
        }
        final File dest = new File(this.file, generation + SUFFIX);
        Files.move(copyFile.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ssTables.clear();
        generation = 0;
        ssTables.put(generation, new SSTable(dest));
        generation++;
        memTable = new MemTable();
    }

    private void flush() throws IOException {
        final File copyFile = new File(this.file, generation + TEMP);
        SSTable.write(copyFile, memTable.iterator(Value.EMPTY_BUFFER);
        final File dest = new File(this.file, generation + SUFFIX);
        Files.move(copyFile.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ssTables.put(generation, new SSTable(dest));
        generation++;
        memTable = new MemTable();
    }

    @NotNull
    private Iterator<Cell> compactIterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iteratorList = new ArrayList<>(ssTables.size() + 1);
        iteratorList.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(table -> {
            try {
                iteratorList.add(table.iterator(from));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        return Iters.collapseEquals(
                Iterators.mergeSorted(iteratorList, Cell.COMPARATOR),
                Cell::getKey);
    }
}

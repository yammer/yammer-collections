package com.yammer.collections.guava.azure.transforming;

import com.google.common.base.Function;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

// TODO : change null handling policy to be consistent (remove null handling in other classes)
// TODO : change type handling policy to be consistent
// TODO : remove duplication, use safe transform


/**
 * // TODO update this doc
 * In early stages of implementation. Built for the Secretie project, which deals with small data sets that can be trivially sent
 * over they wire and stored in memory. In the future it is intended to be generalised to a larger scale.
 */
public class TransformingTable<R, C, V, R1, C1, V1> implements Table<R, C, V> {
    private final Function<Cell<R, C, V>, Cell<R1, C1, V1>> toBaseCellFunction = new Function<Cell<R, C, V>, Cell<R1, C1, V1>>() {
        @Override
        public Cell<R1, C1, V1> apply(Cell<R, C, V> input) {
            return Tables.immutableCell(
                    toRowFunction.apply(input.getRowKey()),
                    toColumnFunction.apply(input.getColumnKey()),
                    toValueFunction.apply(input.getValue())
            );
        }
    };
    private final Function<Cell<R1, C1, V1>, Cell<R, C, V>> fromBaseCellFunction =
            new Function<Cell<R1, C1, V1>, Cell<R, C, V>>() {
                @Override
                public Cell<R, C, V> apply(Cell<R1, C1, V1> input) {
                    return Tables.immutableCell(
                            fromRowFunction.apply(input.getRowKey()),
                            fromColumnFunction.apply(input.getColumnKey()),
                            fromValueFunction.apply(input.getValue())
                    );
                }
            };
    private final Function<C, C1> toColumnFunction;
    private final Function<C1, C> fromColumnFunction;
    private final Function<R, R1> toRowFunction;
    private final Function<R1, R> fromRowFunction;
    private final Function<V, V1> toValueFunction;
    private final Function<V1, V> fromValueFunction;
    private final Table<R1, C1, V1> backingTable;
    private final Function<Map<C, V>, Map<C1, V1>> toRowMapValueFunction;
    private final Function<Map<C1, V1>, Map<C, V>> fromRowMapValueFunction;
    private final Function<Map<R, V>, Map<R1, V1>> toColumnMapValueFunction;
    private final Function<Map<R1, V1>, Map<R, V>> fromColumnMapValueFunction;

    public TransformingTable(
            Table<R1, C1, V1> backingTable,
            Function<R, R1> toRowFunction,
            Function<R1, R> fromRowFunction,
            Function<C, C1> toColumnFunction,
            Function<C1, C> fromColumnFunction,
            Function<V, V1> toValueFunction,
            Function<V1, V> fromValueFunction) {
        this.backingTable = backingTable;
        this.toRowFunction = toRowFunction;
        this.fromRowFunction = fromRowFunction;
        this.toColumnFunction = toColumnFunction;
        this.fromColumnFunction = fromColumnFunction;
        this.toValueFunction = toValueFunction;
        this.fromValueFunction = fromValueFunction;
        toRowMapValueFunction = createMapTransformation(
                fromColumnFunction, toColumnFunction,
                fromValueFunction, toValueFunction
        );
        fromRowMapValueFunction = createMapTransformation(
                toColumnFunction, fromColumnFunction,
                toValueFunction, fromValueFunction
        );
        toColumnMapValueFunction = createMapTransformation(
                fromRowFunction, toRowFunction,
                fromValueFunction, toValueFunction
        );
        fromColumnMapValueFunction = createMapTransformation(
                toRowFunction, fromRowFunction,
                toValueFunction, fromValueFunction
        );
    }

    private static <K, V, K1, V1> Function<Map<K, V>, Map<K1, V1>> createMapTransformation(
            final Function<K1, K> toKeyFunction,
            final Function<K, K1> fromKeyFunction,
            final Function<V1, V> toValueFunction,
            final Function<V, V1> fromValueFunction) {
        return new Function<Map<K, V>, Map<K1, V1>>() {
            @Override
            public Map<K1, V1> apply(java.util.Map<K, V> cvMap) {
                return new TransformingMap<>(
                        cvMap,
                        toKeyFunction, fromKeyFunction,
                        toValueFunction, fromValueFunction
                );
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <F, T> T tryTransforming(Object o, Function<F, T> transfromingFunction) {// TODO take this out, and reuse, add not-null check here?
        try {
            return transfromingFunction.apply((F) o);
        } catch (ClassCastException e) {
            return null;
        }
    }

    @Override
    public boolean contains(Object rowKey, Object columnKey) {
        checkNotNull(rowKey);
        checkNotNull(columnKey);
        final R1 mRowKey = tryTransforming(rowKey, toRowFunction);
        final C1 mColumnKey = tryTransforming(columnKey, toColumnFunction);
        return mRowKey != null &&
                mColumnKey != null &&
                backingTable.contains(mRowKey, mColumnKey);
    }

    @Override
    public boolean containsRow(Object rowKey) {
        checkNotNull(rowKey);
        final R1 mRowKey = tryTransforming(rowKey, toRowFunction);
        return mRowKey != null && backingTable.containsRow(mRowKey);
    }

    @Override
    public boolean containsColumn(Object columnKey) {
        checkNotNull(columnKey);
        final C1 mColumnKey = tryTransforming(columnKey, toColumnFunction);
        return mColumnKey != null && backingTable.containsColumn(mColumnKey);
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value);
        final V1 mValue = tryTransforming(value, toValueFunction);
        return value != null && backingTable.containsValue(mValue);
    }

    @Override
    public V get(Object rowKey, Object columnKey) {
        checkNotNull(rowKey);
        checkNotNull(columnKey);
        final R1 mRowKey = tryTransforming(rowKey, toRowFunction);
        final C1 mColumnKey = tryTransforming(columnKey, toColumnFunction);

        if (mRowKey == null || mColumnKey == null) {
            return null;
        }

        V1 mValue = backingTable.get(mRowKey, mColumnKey);

        return mValue == null ? null : fromValueFunction.apply(mValue);
    }

    @Override
    public boolean isEmpty() {
        return backingTable.isEmpty();
    }

    @Override
    public int size() {
        return backingTable.size();
    }

    @Override
    public void clear() {
        backingTable.clear();
    }

    @Override
    public V put(R rowKey, C columnKey, V value) {
        checkNotNull(rowKey);
        checkNotNull(columnKey);
        checkNotNull(value);

        final V1 mValue = backingTable.put(
                toRowFunction.apply(rowKey),
                toColumnFunction.apply(columnKey),
                toValueFunction.apply(value)
        );

        return mValue == null ? null : fromValueFunction.apply(mValue);
    }

    @Override
    public void putAll(Table<? extends R, ? extends C, ? extends V> table) {
        checkNotNull(table);
        for (Cell<? extends R, ? extends C, ? extends V> cell : table.cellSet()) {
            put(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
        }
    }

    @Override
    public V remove(Object rowKey, Object columnKey) {
        checkNotNull(rowKey);
        checkNotNull(columnKey);
        final R1 mRowKey = tryTransforming(rowKey, toRowFunction);
        final C1 mColumnKey = tryTransforming(columnKey, toColumnFunction);

        if (mRowKey == null || mColumnKey == null) {
            return null;
        }

        final V1 mValue = backingTable.remove(mRowKey, mColumnKey);

        return mValue == null ? null : fromValueFunction.apply(mValue);
    }

    @Override
    public Map<C, V> row(R rowKey) {
        checkNotNull(rowKey);
        return new TransformingMap<>(
                backingTable.row(toRowFunction.apply(rowKey)),
                toColumnFunction, fromColumnFunction,
                toValueFunction, fromValueFunction
        );
    }

    @Override
    public Map<R, V> column(C columnKey) {
        checkNotNull(columnKey);
        return new TransformingMap<>(
                backingTable.column(toColumnFunction.apply(columnKey)),
                toRowFunction, fromRowFunction,
                toValueFunction, fromValueFunction
        );
    }

    @Override
    public Set<Cell<R, C, V>> cellSet() {
        return new TransformingSet<>(
                backingTable.cellSet(),
                toBaseCellFunction,
                fromBaseCellFunction
        );
    }

    @Override
    public Set<R> rowKeySet() {
        return new TransformingSet<>(
                backingTable.rowKeySet(), toRowFunction, fromRowFunction
        );
    }

    @Override
    public Set<C> columnKeySet() {
        return new TransformingSet<>(
                backingTable.columnKeySet(), toColumnFunction, fromColumnFunction
        );
    }

    @Override
    public Collection<V> values() {
        return new TransformingCollection<>(
                backingTable.values(), toValueFunction, fromValueFunction
        );
    }

    @Override
    public Map<R, Map<C, V>> rowMap() {
        return new TransformingMap<>(
                backingTable.rowMap(),
                toRowFunction,
                fromRowFunction,
                toRowMapValueFunction,
                fromRowMapValueFunction
        );
    }

    @Override
    public Map<C, Map<R, V>> columnMap() {

        return new TransformingMap<>(
                backingTable.columnMap(),
                toColumnFunction,
                fromColumnFunction,
                toColumnMapValueFunction,
                fromColumnMapValueFunction
        );
    }

    public static interface Marshaller<F, T> {
        T marshal(F unmarshalled);

        F unmarshal(T marshalled);

        Class<F> getType();
    }
}

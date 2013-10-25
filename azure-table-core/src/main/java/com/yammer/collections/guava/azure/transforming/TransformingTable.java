package com.yammer.collections.guava.azure.transforming;

import com.google.common.base.Function;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * In early stages of implementation. Built for the Secretie project, which deals with small data sets that can be trivially sent
 * over they wire and stored in memory. In the future it is intended to be generalised to a larger scale.
 */
public class TransformingTable<R, C, V, R1, C1, V1> implements Table<R, C, V> {
    private final Function<Cell<R, C, V>, Cell<R1, C1, V1>> toBaseCellFunction = new Function<Cell<R, C, V>, Cell<R1, C1, V1>>() {
        @Override
        public Cell<R1, C1, V1> apply(Cell<R, C, V> input) {
            return Tables.immutableCell(
                    rowKeyMarshaller.marshal(input.getRowKey()),
                    columnKeyMarshaller.marshal(input.getColumnKey()),
                    valueMarshaller.marshal(input.getValue())
            );
        }
    };
    private final Function<Cell<R1, C1, V1>, Cell<R, C, V>> fromBaseCellFunction =
            new Function<Cell<R1, C1, V1>, Cell<R, C, V>>() {
                @Override
                public Cell<R, C, V> apply(Cell<R1, C1, V1> input) {
                    return Tables.immutableCell(
                            rowKeyMarshaller.unmarshal(input.getRowKey()),
                            columnKeyMarshaller.unmarshal(input.getColumnKey()),
                            valueMarshaller.unmarshal(input.getValue())
                    );
                }
            };
    private final Function<C, C1> columnMarshallingTransformation;
    private final Function<C1, C> columnUnmarshallingTransformation;
    private final Function<R, R1> rowMarshallingTransformation;
    private final Function<R1, R> rowUnmarshallingTransformation;
    private final Function<V, V1> valueMarshallingTransformation;
    private final Function<V1, V> valueUnmarshallingTransformation;
    private final Marshaller<R, R1> rowKeyMarshaller;
    private final Marshaller<C, C1> columnKeyMarshaller;
    private final Marshaller<V, V1> valueMarshaller;
    private final Table<R1, C1, V1> backingTable;

    public TransformingTable(final Marshaller<R, R1> rowKeyMarshaller,
                             final Marshaller<C, C1> columnKeyMarshaller,
                             final Marshaller<V, V1> valueMarshaller,
                             Table<R1, C1, V1> backingTable) {
        this.rowKeyMarshaller = rowKeyMarshaller;
        this.columnKeyMarshaller = columnKeyMarshaller;
        this.valueMarshaller = valueMarshaller;
        this.backingTable = backingTable;
        columnMarshallingTransformation = createMarshallingFunction(columnKeyMarshaller);
        columnUnmarshallingTransformation = createUnmarshallingFunction(columnKeyMarshaller);
        rowMarshallingTransformation = createMarshallingFunction(rowKeyMarshaller);
        rowUnmarshallingTransformation = createUnmarshallingFunction(rowKeyMarshaller);
        valueMarshallingTransformation = createMarshallingFunction(valueMarshaller);
        valueUnmarshallingTransformation = createUnmarshallingFunction(valueMarshaller);
    }

    private static <F, T> Function<F, T> createMarshallingFunction(final Marshaller<F, T> marshaller) {
        return new Function<F, T>() {

            @Override
            public T apply(F input) {
                return marshaller.marshal(input);
            }
        };
    }

    private static <F, T> Function<T, F> createUnmarshallingFunction(final Marshaller<F, T> marshaller) {
        return new Function<T, F>() {

            @Override
            public F apply(T input) {
                return marshaller.unmarshal(input);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <F, T> T tryMarshaling(Object o, Marshaller<F, T> marshaller) {
        if (marshaller.getType().equals(o.getClass())) {
            return marshaller.marshal((F) o);
        }
        return null;
    }

    @Override
    public boolean contains(Object rowKey, Object columnKey) {
        checkNotNull(rowKey);
        checkNotNull(columnKey);
        final R1 mRowKey = tryMarshaling(rowKey, rowKeyMarshaller);
        final C1 mColumnKey = tryMarshaling(columnKey, columnKeyMarshaller);
        return mRowKey != null &&
                mColumnKey != null &&
                backingTable.contains(mRowKey, mColumnKey);
    }

    @Override
    public boolean containsRow(Object rowKey) {
        checkNotNull(rowKey);
        final R1 mRowKey = tryMarshaling(rowKey, rowKeyMarshaller);
        return mRowKey != null && backingTable.containsRow(mRowKey);
    }

    @Override
    public boolean containsColumn(Object columnKey) {
        checkNotNull(columnKey);
        final C1 mColumnKey = tryMarshaling(columnKey, columnKeyMarshaller);
        return mColumnKey != null && backingTable.containsColumn(mColumnKey);
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value);
        final V1 mValue = tryMarshaling(value, valueMarshaller);
        return value != null && backingTable.containsValue(mValue);
    }

    @Override
    public V get(Object rowKey, Object columnKey) {
        checkNotNull(rowKey);
        checkNotNull(columnKey);
        final R1 mRowKey = tryMarshaling(rowKey, rowKeyMarshaller);
        final C1 mColumnKey = tryMarshaling(columnKey, columnKeyMarshaller);

        if (mRowKey == null || mColumnKey == null) {
            return null;
        }

        V1 mValue = backingTable.get(mRowKey, mColumnKey);

        return mValue == null ? null : valueMarshaller.unmarshal(mValue);
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
                rowKeyMarshaller.marshal(rowKey),
                columnKeyMarshaller.marshal(columnKey),
                valueMarshaller.marshal(value)
        );

        return mValue == null ? null : valueMarshaller.unmarshal(mValue);
    }

    @Override
    public void putAll(Table<? extends R, ? extends C, ? extends V> table) {
        for (Cell<? extends R, ? extends C, ? extends V> cell : table.cellSet()) {
            put(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
        }
    }

    @Override
    public V remove(Object rowKey, Object columnKey) {
        checkNotNull(rowKey);
        checkNotNull(columnKey);
        final R1 mRowKey = tryMarshaling(rowKey, rowKeyMarshaller);
        final C1 mColumnKey = tryMarshaling(columnKey, columnKeyMarshaller);

        if (mRowKey == null || mColumnKey == null) {
            return null;
        }

        final V1 mValue = backingTable.remove(mRowKey, mColumnKey);

        return mValue == null ? null : valueMarshaller.unmarshal(mValue);
    }

    @Override
    public Map<C, V> row(R rowKey) {
        throw new UnsupportedOperationException();   // TODO implement
    }

    @Override
    public Map<R, V> column(C columnKey) {
        throw new UnsupportedOperationException();    // TODO implement
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
        return new TransformingSet(
                backingTable.rowKeySet(), rowMarshallingTransformation, rowUnmarshallingTransformation
        );
    }

    @Override
    public Set<C> columnKeySet() {
        return new TransformingSet(
                backingTable.columnKeySet(), columnMarshallingTransformation, columnUnmarshallingTransformation
        );
    }

    @Override
    public Collection<V> values() {
        return new TransformingCollection<>(
                backingTable.values(), valueMarshallingTransformation, valueUnmarshallingTransformation
        );
    }

    @Override
    public Map<R, Map<C, V>> rowMap() {
        throw new UnsupportedOperationException();    // TODO implement
    }

    @Override
    public Map<C, Map<R, V>> columnMap() {
        throw new UnsupportedOperationException();     // TODO implement
    }

    public static interface Marshaller<F, T> {
        T marshal(F unmarshalled);

        F unmarshal(T marshalled);

        Class<F> getType();
    }
}

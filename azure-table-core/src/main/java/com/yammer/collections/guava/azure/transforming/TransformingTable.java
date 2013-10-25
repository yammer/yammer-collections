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
// TODO transforming table, make it really agnostic of the target
public class TransformingTable<R, C, V> implements Table<R, C, V> {
    private final Function<Cell<R, C, V>, Cell<String, String, String>> toBaseCellFunction = new Function<Cell<R, C, V>, Cell<String, String, String>>() {
        @Override
        public Cell<String, String, String> apply(Cell<R, C, V> input) {
            return Tables.immutableCell(
                    rowKeyMarshaller.marshal(input.getRowKey()),
                    columnKeyMarshaller.marshal(input.getColumnKey()),
                    valueMarshaller.marshal(input.getValue())
            );
        }
    };
    private final Function<Cell<String, String, String>, Cell<R, C, V>> fromBaseCellFunction =
            new Function<Cell<String, String, String>, Cell<R, C, V>>() {
                @Override
                public Cell<R, C, V> apply(Cell<String, String, String> input) {
                    return Tables.immutableCell(
                            rowKeyMarshaller.unmarshal(input.getRowKey()),
                            columnKeyMarshaller.unmarshal(input.getColumnKey()),
                            valueMarshaller.unmarshal(input.getValue())
                    );
                }
            };
    private final Function<C, String> columnMarshallingTransformation;
    private final Function<String, C> columnUnmarshallingTransformation;
    private final Function<R, String> rowMarshallingTransformation;
    private final Function<String, R> rowUnmarshallingTransformation;
    private final Function<V, String> valueMarshallingTransformation;
    private final Function<String, V> valueUnmarshallingTransformation;

    private final Marshaller<R, String> rowKeyMarshaller;
    private final Marshaller<C, String> columnKeyMarshaller;
    private final Marshaller<V, String> valueMarshaller;
    private final Table<String, String, String> backingTable;

    public TransformingTable(final Marshaller<R, String> rowKeyMarshaller,
                             final Marshaller<C, String> columnKeyMarshaller,
                             final Marshaller<V, String> valueMarshaller,
                             Table<String, String, String> backingTable) {
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
        final String rowKeyString = tryMarshaling(rowKey, rowKeyMarshaller);
        final String columnKeyString = tryMarshaling(columnKey, columnKeyMarshaller);
        return rowKeyString != null &&
                columnKeyString != null &&
                backingTable.contains(rowKeyString, columnKeyString);
    }

    @Override
    public boolean containsRow(Object rowKey) {
        checkNotNull(rowKey);
        final String rowKeyString = tryMarshaling(rowKey, rowKeyMarshaller);
        return rowKeyString != null && backingTable.containsRow(rowKeyString);
    }

    @Override
    public boolean containsColumn(Object columnKey) {
        checkNotNull(columnKey);
        final String columnKeyString = tryMarshaling(columnKey, columnKeyMarshaller);
        return columnKeyString != null && backingTable.containsColumn(columnKeyString);
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value);
        final String valueString = tryMarshaling(value, valueMarshaller);
        return value != null && backingTable.containsValue(valueString);
    }

    @Override
    public V get(Object rowKey, Object columnKey) {
        checkNotNull(rowKey);
        checkNotNull(columnKey);
        final String rowKeyString = tryMarshaling(rowKey, rowKeyMarshaller);
        final String columnKeyString = tryMarshaling(columnKey, columnKeyMarshaller);

        if (rowKeyString == null || columnKeyString == null) {
            return null;
        }

        String valueString = backingTable.get(rowKeyString, columnKeyString);

        return valueString == null ? null : valueMarshaller.unmarshal(valueString);
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

        final String valueString = backingTable.put(
                rowKeyMarshaller.marshal(rowKey),
                columnKeyMarshaller.marshal(columnKey),
                valueMarshaller.marshal(value)
        );

        return valueString == null ? null : valueMarshaller.unmarshal(valueString);
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
        final String rowKeyString = tryMarshaling(rowKey, rowKeyMarshaller);
        final String columnKeyString = tryMarshaling(columnKey, columnKeyMarshaller);

        if (rowKeyString == null || columnKeyString == null) {
            return null;
        }

        final String valueString = backingTable.remove(rowKeyString, columnKeyString);

        return valueString == null ? null : valueMarshaller.unmarshal(valueString);
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

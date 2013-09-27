package com.yammer.collections.guava.azure;

import com.google.common.base.Function;
import com.google.common.collect.*;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * In early stages of implementation. Built for the Secretie project, which deals with small data sets that can be trivially sent
 * over they wire and stored in memory. In the future it is intended to be generalised to a larger scale.
 */
public class AzureTable<R, C, V> implements Table<R, C, V> {
    private final Function<Cell<String, String, String>, Cell<R, C, V>> cellUnmarshallingTransformation =
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
    private final Function<String, C> columnUnmarshallingTransformation = new Function<String, C>() {
        @Override
        public C apply(String input) {
            return columnKeyMarshaller.unmarshal(input);
        }
    };

    private final Marshaller<R, String> rowKeyMarshaller;
    private final Marshaller<C, String> columnKeyMarshaller;
    private final Marshaller<V, String> valueMarshaller;
    private final Table<String, String, String> backingTable;

    public AzureTable(Marshaller<R, String> rowKeyMarshaller,
                      Marshaller<C, String> columnKeyMarshaller,
                      Marshaller<V, String> valueMarshaller,
                      Table<String, String, String> backingTable) {
        this.rowKeyMarshaller = rowKeyMarshaller;
        this.columnKeyMarshaller = columnKeyMarshaller;
        this.valueMarshaller = valueMarshaller;
        this.backingTable = backingTable;
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
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<R, V> column(C columnKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Cell<R, C, V>> cellSet() { // TODO very simple implementatin: not live, not mutable
        return ImmutableSet.copyOf(Iterables.transform(backingTable.cellSet(), cellUnmarshallingTransformation));
    }

    @Override
    public Set<R> rowKeySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<C> columnKeySet() {
        return ImmutableSet.copyOf(Iterables.transform(backingTable.columnKeySet(), columnUnmarshallingTransformation));
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<R, Map<C, V>> rowMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<C, Map<R, V>> columnMap() {
        throw new UnsupportedOperationException();
    }

    public static interface Marshaller<F, T> {
        T marshal(F unmarshalled);

        F unmarshal(T marshalled);

        Class<F> getType();
    }
}

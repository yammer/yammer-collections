package com.yammer.collections.guava.azure;


import com.google.common.collect.Iterables;
import com.google.common.collect.Table;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

class StringEtityIterableSet implements Set<Table.Cell<String, String, String>> {
    private final StringAzureTable stringAzureTable;
    private final Iterable<StringEntity> stringEntityIterable;

    StringEtityIterableSet(StringAzureTable azureTable, Iterable<StringEntity> stringEntityIterable) {
        stringAzureTable = azureTable;
        this.stringEntityIterable = stringEntityIterable;
    }

    @Override
    public int size() {
        // TODO can this be optimized with a direct query?
        return Iterables.size(stringEntityIterable);
    }

    @Override
    public boolean isEmpty() {
        // TODO can this be optimized with a direct query?
        return Iterables.isEmpty(stringEntityIterable);
    }

    @Override
    public boolean contains(Object o) {
        if(o instanceof Table.Cell) {
            Table.Cell<Object, Object, Object> cell = (Table.Cell<Object, Object, Object>) o;
            return stringAzureTable.contains(cell.getRowKey(), cell.getColumnKey());
        }

        return false;
    }

    @Override
    public Iterator<Table.Cell<String, String, String>> iterator() {
        throw new UnsupportedOperationException();// TODO implement this
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();// TODO implement this
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();// TODO implement this
    }

    @Override
    public boolean add(Table.Cell<String, String, String> stringStringStringCell) {
        throw new UnsupportedOperationException();// TODO implement this
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();// TODO implement this
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();// TODO implement this
    }

    @Override
    public boolean addAll(Collection<? extends Table.Cell<String, String, String>> c) {
        throw new UnsupportedOperationException();// TODO implement this
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();// TODO implement this
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();// TODO implement this
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();// TODO implement this
    }
}

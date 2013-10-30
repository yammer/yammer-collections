package com.yammer.collections.guava.azure.metrics;

import com.google.common.collect.Table;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Provides metrics for the core operations, but not for the collections view operations.
 * @param <R>
 * @param <C>
 * @param <V>
 */
public class MeteredTable<R,C,V> implements Table<R,C,V> {
    private final Table<R,C,V> backingTable;

    public static <R,C,V> Table<R,C,V> create(Table<R,C,V> backingTable) {
        return new MeteredTable<>(backingTable);
    }

    private MeteredTable(Table<R,C,V> backingTable) {
        this.backingTable = backingTable;
    }

    @Override
    public boolean contains(Object o, Object o2) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsRow(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsColumn(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsValue(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public V get(Object o, Object o2) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isEmpty() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int size() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clear() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public V put(R r, C c, V v) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void putAll(Table<? extends R, ? extends C, ? extends V> table) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public V remove(Object o, Object o2) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<C, V> row(R r) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<R, V> column(C c) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<Cell<R, C, V>> cellSet() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<R> rowKeySet() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<C> columnKeySet() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Collection<V> values() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<R, Map<C, V>> rowMap() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<C, Map<R, V>> columnMap() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

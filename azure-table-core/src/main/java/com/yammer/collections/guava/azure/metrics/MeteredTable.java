package com.yammer.collections.guava.azure.metrics;

import com.google.common.collect.Table;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides metrics for the core operations, but not for the collections view operations.
 * Operations include: get, put, remove, size, clear
 *
 * @param <R>
 * @param <C>
 * @param <V>
 */
@SuppressWarnings("ClassWithTooManyMethods")
public class MeteredTable<R, C, V> implements Table<R, C, V> {
    private static final Timer GET_TIMER = createTimerFor("get");
    private static final Timer PUT_TIMER = createTimerFor("put");
    private static final Timer REMOVE_TIMER = createTimerFor("remove");
    private static final Timer SIZE_TIMER = createTimerFor("size");
    private static final Timer CLEAR_TIMER = createTimerFor("clear");
    private final Table<R, C, V> backingTable;

    private MeteredTable(Table<R, C, V> backingTable) {
        this.backingTable = checkNotNull(backingTable);
    }

    private static Timer createTimerFor(String name) {
        return Metrics.newTimer(MeteredTable.class, name);
    }

    public static <R, C, V> Table<R, C, V> create(Table<R, C, V> backingTable) {
        return new MeteredTable<>(backingTable);
    }

    @Override
    public boolean contains(Object o, Object o2) {
        return backingTable.contains(o, o2);
    }

    @Override
    public boolean containsRow(Object o) {
        return backingTable.containsRow(o);
    }

    @Override
    public boolean containsColumn(Object o) {
        return backingTable.containsColumn(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return backingTable.containsValue(o);
    }

    @Override
    public V get(Object o, Object o2) {
        TimerContext ctx = GET_TIMER.time();
        try {
            return backingTable.get(o, o2);
        } finally {
            ctx.stop();
        }
    }

    @Override
    public boolean isEmpty() {
        return backingTable.isEmpty();
    }

    @Override
    public int size() {
        TimerContext ctx = SIZE_TIMER.time();
        try {
            return backingTable.size();
        } finally {
            ctx.stop();
        }
    }

    @Override
    public void clear() {
        TimerContext ctx = CLEAR_TIMER.time();
        try {
            backingTable.clear();
        } finally {
            ctx.stop();
        }
    }

    @Override
    public V put(R r, C c, V v) {
        TimerContext ctx = PUT_TIMER.time();
        try {
            return backingTable.put(r, c, v);
        } finally {
            ctx.stop();
        }
    }

    @Override
    public void putAll(Table<? extends R, ? extends C, ? extends V> table) {
        backingTable.putAll(table);
    }

    @Override
    public V remove(Object o, Object o2) {
        TimerContext ctx = REMOVE_TIMER.time();
        try {
            return backingTable.remove(o, o2);
        } finally {
            ctx.stop();
        }
    }

    @Override
    public Map<C, V> row(R r) {
        return backingTable.row(r);
    }

    @Override
    public Map<R, V> column(C c) {
        return backingTable.column(c);
    }

    @Override
    public Set<Cell<R, C, V>> cellSet() {
        return backingTable.cellSet();
    }

    @Override
    public Set<R> rowKeySet() {
        return backingTable.rowKeySet();
    }

    @Override
    public Set<C> columnKeySet() {
        return backingTable.columnKeySet();
    }

    @Override
    public Collection<V> values() {
        return backingTable.values();
    }

    @Override
    public Map<R, Map<C, V>> rowMap() {
        return backingTable.rowMap();
    }

    @Override
    public Map<C, Map<R, V>> columnMap() {
        return backingTable.columnMap();
    }
}

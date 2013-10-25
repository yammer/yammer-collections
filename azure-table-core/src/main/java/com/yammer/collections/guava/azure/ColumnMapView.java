package com.yammer.collections.guava.azure;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Table;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ColumnMapView<R, C, V> implements Map<C, Map<R, V>> {
    private final Table<R, C, V> backingTable;

    public ColumnMapView(Table<R, C, V> backingTable) {
        this.backingTable = backingTable;
    }


    @Override
    public int size() {
        return backingTable.columnKeySet().size();
    }

    @Override
    public boolean isEmpty() {
        return backingTable.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return backingTable.containsColumn(key);
    }

    @Override
    public boolean containsValue(Object value) {
        if (!(value instanceof Entry)) {
            return false;
        }

        Entry<?, ?> entry = (Entry<?, ?>) value;
        try {
            return backingTable.row((R) entry.getKey()).containsValue(entry.getValue());
        } catch (ClassCastException c) {
            return false;
        }
    }

    @Override
    public Map<R, V> get(Object key) {
        try {
            Map<R, V> mapForRow = backingTable.column((C) key);
            return mapForRow.isEmpty() ? null : mapForRow;
        } catch (ClassCastException e) {
            return null;
        }
    }

    @Override
    public Map<R, V> put(C key, Map<R, V> value) {// TODO ret value breaks contract
        Map<R, V> oldValue = get(key);
        oldValue.clear();
        if (!value.isEmpty()) {
            backingTable.column(key).putAll(value);
        }
        return oldValue;
    }

    @Override
    public Map<R, V> remove(Object key) {// TODO ret value breaks contract
        Map<R, V> oldValue = get(key);
        oldValue.clear();
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends C, ? extends Map<R, V>> m) {
        for (Entry<? extends C, ? extends Map<R, V>> entry : m.entrySet()) {
            backingTable.column(entry.getKey()).putAll(entry.getValue());
        }
    }

    @Override
    public void clear() {
        backingTable.clear();
    }

    @Override
    public Set<C> keySet() {
        return backingTable.columnKeySet();
    }

    @Override
    public Collection<Map<R, V>> values() {
        // TODO make this static or at least once per instance
        return Collections2.transform(
                keySet(),
                new Function<C, Map<R, V>>() {
                    @Override
                    public Map<R, V> apply(final C key) {
                        return backingTable.column(key);
                    }
                }
        );
    }

    @Override
    public Set<Entry<C, Map<R, V>>> entrySet() {
        // TODO make this static or at least once per instance
        return new HashSet<>(// TODO this is temporary, materializes
                Collections2.transform(
                        keySet(),
                        new Function<C, Entry<C, Map<R, V>>>() {
                            @Override
                            public Entry<C, Map<R, V>> apply(final C input) {
                                // TODO make it static
                                return new Entry<C, Map<R, V>>() {
                                    @Override
                                    public C getKey() {
                                        return input;
                                    }

                                    @Override
                                    public Map<R, V> getValue() {
                                        return backingTable.column(input);
                                    }

                                    @Override
                                    public Map<R, V> setValue(Map<R, V> value) {
                                        return put(input, value);
                                    }

                                };
                            }
                        }
                ));
    }
}

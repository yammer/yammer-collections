package com.yammer.collections.guava.azure;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Table;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class RowMapView<R, C, V> implements Map<R, Map<C, V>> {
    private final Table<R, C, V> backingTable;
    private final Function<R, Map<C, V>> valueCreator;
    private final Function<R, Entry<R, Map<C, V>>> entryCreator;

    public RowMapView(final Table<R, C, V> backingTable) {
        this.backingTable = backingTable;
        valueCreator = new Function<R, Map<C, V>>() {
            @Override
            public Map<C, V> apply(final R key) {
                return backingTable.row(key);
            }
        };
        entryCreator = new

                Function<R, Entry<R, Map<C, V>>>() {
                    @Override
                    public Entry<R, Map<C, V>> apply(final R input) {
                        return new Entry<R, Map<C, V>>() {
                            @Override
                            public R getKey() {
                                return input;
                            }

                            @Override
                            public Map<C, V> getValue() {
                                return backingTable.row(input);
                            }

                            @Override
                            public Map<C, V> setValue(Map<C, V> value) {
                                return put(input, value);
                            }

                        };
                    }
                };

    }

    @Override
    public int size() {
        return backingTable.rowKeySet().size();
    }

    @Override
    public boolean isEmpty() {
        return backingTable.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return backingTable.containsRow(key);
    }

    @Override
    public boolean containsValue(Object value) {
        if (!(value instanceof Entry)) {
            return false;
        }

        Entry<?, ?> entry = (Entry<?, ?>) value;
        try {
            return backingTable.column((C) entry.getKey()).containsValue(entry.getValue());
        } catch (ClassCastException c) {
            return false;
        }
    }

    @Override
    public Map<C, V> get(Object key) {
        try {
            Map<C, V> mapForRow = backingTable.row((R) key);
            return mapForRow.isEmpty() ? null : mapForRow;
        } catch (ClassCastException e) {
            return null;
        }
    }

    @Override
    public Map<C, V> put(R key, Map<C, V> value) {// TODO ret value breaks contract
        Map<C, V> oldValue = get(key);
        oldValue.clear();
        if (!value.isEmpty()) {
            backingTable.row(key).putAll(value);
        }
        return oldValue;
    }

    @Override
    public Map<C, V> remove(Object key) {// TODO ret value breaks contract
        Map<C, V> oldValue = get(key);
        oldValue.clear();
        return oldValue;
    }

    @Override
    public void putAll(Map<? extends R, ? extends Map<C, V>> m) {
        for (Entry<? extends R, ? extends Map<C, V>> entry : m.entrySet()) {
            backingTable.row(entry.getKey()).putAll(entry.getValue());
        }
    }

    @Override
    public void clear() {
        backingTable.clear();
    }

    @Override
    public Set<R> keySet() {
        return backingTable.rowKeySet();
    }

    @Override
    public Collection<Map<C, V>> values() {
        return Collections2.transform(
                keySet(),
                valueCreator
        );
    }

    @Override
    public Set<Entry<R, Map<C, V>>> entrySet() {
        return new HashSet<>(// TODO this is temporary, materializes
                Collections2.transform(
                        keySet(),
                        entryCreator
                ));
    }
}

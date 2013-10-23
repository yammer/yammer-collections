package com.yammer.collections.guava.azure;


import java.util.Collection;
import java.util.Map;
import java.util.Set;

class ColumnMap implements Map<String, String> {
    private final StringAzureTable stringAzureTable;
    private final String rowKey;

    public ColumnMap(StringAzureTable stringAzureTable, String rowKey) {

        this.stringAzureTable = stringAzureTable;
        this.rowKey = rowKey;
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        return stringAzureTable.contains(rowKey, key);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String get(Object key) {
        return stringAzureTable.get(rowKey, key);
    }

    @Override
    public String put(String key, String value) {
        return stringAzureTable.put(rowKey, key, value);
    }

    @Override
    public String remove(Object key) {
        return stringAzureTable.remove(rowKey, key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<String> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        throw new UnsupportedOperationException();
    }
}

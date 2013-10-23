package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.microsoft.windowsazure.services.table.client.TableQuery;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.yammer.collections.guava.azure.StringEntityUtil.decode;

// TODO no timers here as of yet
class ColumnMap implements Map<String, String> {
    // TODO these are probably extractable
    private static final Function<? super StringEntity, ? extends String> EXTRACT_VALUE = new Function<StringEntity, String>() {
        @Override
        public String apply(StringEntity input) {
            return decode(input.getValue());
        }
    };
    private static final Function<? super StringEntity, ? extends String> EXTRACT_COLUMN_KEY = new Function<StringEntity, String>() {
        @Override
        public String apply(StringEntity input) {
            return decode(input.getRowKey());
        }
    };
    private static final Function<? super StringEntity, ? extends Entry<String, String>> EXTRACT_ENTRY = new Function<StringEntity, Entry<String, String>>() {
        @Override
        public Entry<String, String> apply(StringEntity input) {
            return new ColumnMapEntry(decode(input.getRowKey()), decode(input.getValue()));
        }
    };
    private final StringAzureTable stringAzureTable;
    private final String rowKey;
    private final StringTableCloudClient stringTableCloudClientMock;
    private final StringTableRequestFactory stringTableRequestFactoryMock;

    public ColumnMap(StringAzureTable stringAzureTable,
                     String rowKey,
                     StringTableCloudClient stringTableCloudClientMock,
                     StringTableRequestFactory stringTableRequestFactoryMock) {
        this.stringAzureTable = stringAzureTable;
        this.rowKey = rowKey;
        this.stringTableCloudClientMock = stringTableCloudClientMock;
        this.stringTableRequestFactoryMock = stringTableRequestFactoryMock;
    }

    @Override // TODO this requires a javadoc to explain that this is a very expensive operation
    public int size() { // investigate, maybe there is a single azure op that can do that
        return entrySet().size();
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
        for (Entry<? extends String, ? extends String> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {// TODO this requires a javadoc to explain that this is a very expensive operation
       for(String columnKey : keySet()) {
           remove(columnKey);
       }
    }

    @Override
    public Set<String> keySet() {
        // TODO: we are drainging the whole iterable into a set here: this should be replaced with a view
        TableQuery<StringEntity> selectAllForRowQuery = stringTableRequestFactoryMock.selectAllForRow(stringAzureTable.getTableName(), rowKey);
        Iterable<String> columnStringIterable = Iterables.transform(stringTableCloudClientMock.execute(selectAllForRowQuery), EXTRACT_COLUMN_KEY);
        return Collections.unmodifiableSet(Sets.newHashSet(columnStringIterable));
    }

    // TODO all three methods can be done using a dynamic collection view

    @Override
    public Collection<String> values() {
        // TODO: we are drainging the whole iterable into a memmory here: this should be replaced with a view
        TableQuery<StringEntity> selectAllForRowQuery = stringTableRequestFactoryMock.selectAllForRow(stringAzureTable.getTableName(), rowKey);
        Iterable<String> valuesStringIterable = Iterables.transform(stringTableCloudClientMock.execute(selectAllForRowQuery), EXTRACT_VALUE);
        return ImmutableList.copyOf(valuesStringIterable.iterator());
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        // TODO: we are drainging the whole iterable into a memmory here: this should be replaced with a view
        TableQuery<StringEntity> selectAllForRowQuery = stringTableRequestFactoryMock.selectAllForRow(stringAzureTable.getTableName(), rowKey);
        Iterable<Entry<String, String>> stringEntries = Iterables.transform(stringTableCloudClientMock.execute(selectAllForRowQuery), EXTRACT_ENTRY);
        return Collections.unmodifiableSet(Sets.newHashSet(stringEntries));
    }

    private static class ColumnMapEntry implements Entry<String, String> {
        private final String key;
        private final String value;

        private ColumnMapEntry(String key, String value) {

            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String setValue(String value) {
            throw new UnsupportedOperationException();  //TODO implement this
        }
    }
}

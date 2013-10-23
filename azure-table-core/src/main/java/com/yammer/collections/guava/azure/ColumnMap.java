package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
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
    private static final Function<? super StringEntity, ? extends String> EXTRACT_COLUMN_KEY = new Function<StringEntity, String>() {
        @Override
        public String apply(StringEntity input) {
            return decode(input.getRowKey());
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

    @Override
    public int size() {  // TODO can we actually support this?
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
        for (Entry<? extends String, ? extends String> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {// TODO can we actually support this?
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        // TODO: we are drainging the whole iterable into a set here: this should be replaced with a view
        TableQuery<StringEntity> selectAllForRowQuery = stringTableRequestFactoryMock.selectAllForRow(stringAzureTable.getTableName(), rowKey);
        Iterable<String> columnStringIterable = Iterables.transform(stringTableCloudClientMock.execute(selectAllForRowQuery), EXTRACT_COLUMN_KEY);
        return Collections.unmodifiableSet(Sets.newHashSet(columnStringIterable));

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

package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.microsoft.windowsazure.services.table.client.TableQuery;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static com.yammer.collections.guava.azure.StringEntityUtil.decode;

class StringEtityIterableSet implements Set<Table.Cell<String, String, String>> {
    private static final Function<StringEntity, Table.Cell<String, String, String>> TABLE_CELL_CREATOR =
            new Function<StringEntity, Table.Cell<String, String, String>>() {
                @Override
                public Table.Cell<String, String, String> apply(StringEntity input) {
                    return Tables.immutableCell(
                            decode(input.getPartitionKey()),
                            decode(input.getRowKey()),
                            decode(input.getValue()));
                }
            };

    private final StringAzureTable stringAzureTable;
    private final Iterable<StringEntity> stringEntityIterable;
    private final StringTableCloudClient stringCloudTableClient;
    private final StringTableRequestFactory stringTableRequestFactory;

    StringEtityIterableSet(StringAzureTable azureTable, Iterable<StringEntity> stringEntityIterable,
                           StringTableCloudClient stringCloudTableClient,
                           StringTableRequestFactory stringTableRequestFactory) {
        stringAzureTable = azureTable;
        this.stringEntityIterable = stringEntityIterable;
        this.stringCloudTableClient = stringCloudTableClient;
        this.stringTableRequestFactory = stringTableRequestFactory;
    }

    @Override
    public int size() {
        // TODO migrate it to method call on self,
        // TODO can this be optimized through a direct query
        return Iterables.size(stringEntityIterable);
    }

    @Override
    public boolean isEmpty() {
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
        TableQuery<StringEntity> query = stringTableRequestFactory.selectAll(stringAzureTable.getTableName());
        return Iterables.transform(
                stringCloudTableClient.execute(query),
                TABLE_CELL_CREATOR).iterator();
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

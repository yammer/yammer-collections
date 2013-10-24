package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.microsoft.windowsazure.services.table.client.TableQuery;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static com.yammer.collections.guava.azure.StringEntityUtil.decode;
     // TODO should be renamed, as it allows modifications
/**
 * This class implements the set interface, however it does not enforce it as it only a view.
 */
class CellSetView extends AbstractSet<Table.Cell<String, String, String>> implements Set<Table.Cell<String, String, String>> {
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
    private final StringTableCloudClient stringCloudTableClient;
    private final StringTableRequestFactory stringTableRequestFactory;

    CellSetView(StringAzureTable azureTable,
                StringTableCloudClient stringCloudTableClient,
                StringTableRequestFactory stringTableRequestFactory) {
        stringAzureTable = azureTable;
        this.stringCloudTableClient = stringCloudTableClient;
        this.stringTableRequestFactory = stringTableRequestFactory;
    }

    @Override
    public int size() {
        // TODO can this be optimized through a direct query
        return Iterables.size(getBackingIterable());
    }

    @Override
    public boolean isEmpty() {
        return !iterator().hasNext();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof Table.Cell) {
            Table.Cell<Object, Object, Object> cell = (Table.Cell<Object, Object, Object>) o;
            return stringAzureTable.contains(cell.getRowKey(), cell.getColumnKey());
        }

        return false;
    }

    private Iterable<StringEntity> getBackingIterable() {
        TableQuery<StringEntity> query = stringTableRequestFactory.selectAll(stringAzureTable.getTableName());
        return stringCloudTableClient.execute(query);
    }

    @Override
    public Iterator<Table.Cell<String, String, String>> iterator() {
        return Iterables.transform(
                getBackingIterable(),
                TABLE_CELL_CREATOR).iterator();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Table.Cell<String, String, String> cell) {
        return stringAzureTable.put(
                cell.getRowKey(),
                cell.getColumnKey(),
                cell.getValue()
        ) == null;
    }

    @Override
    public boolean remove(Object o) {
        if(!(o instanceof Table.Cell)) {
            return false;
        }

        Table.Cell<Object, Object, Object> cell = (Table.Cell) o;

        return stringAzureTable.remove(
                cell.getRowKey(),
                cell.getColumnKey()
        ) != null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for(Object o : c) {
            if(!contains(o)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean addAll(Collection<? extends Table.Cell<String, String, String>> c) {
        boolean change = false;
        for(Table.Cell<String, String, String> cell : c) {
            if(add(cell) == true) {
                change = true;
            }
        }

        return change;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean change = false;
        for(Object o : c) {
            if(remove(o) == true) {
                change = true;
            }
        }

        return change;
    }

    @Override
    public void clear() {
        removeAll(this); // this works, because the iterator is only a view onto a remote collection
    }

    @Override
    public String toString() {
        return super.toString()+"AZURE_TABLE_NAME: "+stringAzureTable.getTableName();
    }
}

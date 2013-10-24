package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.microsoft.windowsazure.services.table.client.TableQuery;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

public class CollectionView<E> extends AbstractCollection<E> {
    private final StringAzureTable stringAzureTable;
    private final Function<StringEntity, E> typeExtractor;
    private final StringTableCloudClient stringTableCloudClient;
    private final StringTableRequestFactory stringTableRequestFactory;

    public CollectionView(
            StringAzureTable stringAzureTable,
                          Function<StringEntity, E> typeExtractor,
                          StringTableCloudClient stringTableCloudClient,
                          StringTableRequestFactory stringTableRequestFactory
    ) {
        this.stringAzureTable = stringAzureTable;
        this.typeExtractor = typeExtractor;
        this.stringTableCloudClient = stringTableCloudClient;
        this.stringTableRequestFactory = stringTableRequestFactory;
    }

    @Override
    public int size() {
        return Iterables.size(getBackingIterable());
    }

    @Override
    public boolean isEmpty() {
        return !iterator().hasNext();
    }

    @Override
    public boolean contains(Object o) {
        return Iterables.contains(
                Iterables.transform(getBackingIterable(), typeExtractor),
                o);
    }

    private Iterable<StringEntity> getBackingIterable() {
        TableQuery<StringEntity> query = stringTableRequestFactory.selectAll(stringAzureTable.getTableName());
        return stringTableCloudClient.execute(query);
    }

    @Override
    public Iterator<E> iterator() {
        return Iterables.transform(
                getBackingIterable(),
                typeExtractor).iterator();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}

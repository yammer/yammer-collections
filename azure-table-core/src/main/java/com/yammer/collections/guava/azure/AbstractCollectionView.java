package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

abstract class AbstractCollectionView<E> extends AbstractCollection<E> {
    private final Function<AzureEntity, E> typeExtractor;

    protected AbstractCollectionView(Function<AzureEntity, E> typeExtractor) {
        this.typeExtractor = typeExtractor;
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

    protected abstract Iterable<AzureEntity> getBackingIterable();

    @SuppressWarnings("NullableProblems")
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

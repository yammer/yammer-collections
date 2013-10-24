package com.yammer.collections.guava.azure;


import com.google.common.base.Function;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

public class SetView<E> extends AbstractSet<E> {
    private final CollectionView<E> collectionView;

    public SetView(CollectionView<E> collectionView) {
        this.collectionView = collectionView;
    }

    @Override
    public int size() {
        return collectionView.size();
    }

    @Override
    public boolean isEmpty() {
        return collectionView.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return collectionView.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return collectionView.iterator();
    }

    @Override
    public boolean remove(Object o) {
        return collectionView.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return collectionView.containsAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return collectionView.removeAll(c);
    }

    @Override
    public void clear() {
        collectionView.clear();
    }
}

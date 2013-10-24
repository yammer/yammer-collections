package com.yammer.collections.guava.azure;


import java.util.Collection;
import java.util.Iterator;

public class CollectionView<E> implements Collection<E> {
    @Override
    public int size() {
        return 0;  //TODO implement this
    }

    @Override
    public boolean isEmpty() {
        return false;  //TODO implement this
    }

    @Override
    public boolean contains(Object o) {
        return false;  //TODO implement this
    }

    @Override
    public Iterator<E> iterator() {
        return null;  //TODO implement this
    }

    @Override
    public Object[] toArray() {
        return new Object[0];  //TODO implement this
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;  //TODO implement this
    }

    @Override
    public boolean add(E e) {
        return false;  //TODO implement this
    }

    @Override
    public boolean remove(Object o) {
        return false;  //TODO implement this
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;  //TODO implement this
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return false;  //TODO implement this
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;  //TODO implement this
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;  //TODO implement this
    }

    @Override
    public void clear() {
        //TODO implement this
    }
}

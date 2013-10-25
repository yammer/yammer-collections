package com.yammer.collections.guava.azure;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class TransformingSet<F, T> extends AbstractSet<F> implements Set<F> {
    private final Set<T> backingSet;
    private final Function<F, T> toFunction;
    private final Function<T, F> fromFunction;

    /**
     * This implementation will break if the following is not satisfied:
     *
     * - for every element F f, fromFunction(toFunction(f)) = f
     * - for every element T f, toFunction(FromFunction(t)) = t
     *
     * i.e., fromFunction is a bijection and the toFunction is its reverse
     */
    public TransformingSet(Set<T> backingSet, Function<F, T> toFunction, Function<T, F> fromFunction) {
        this.backingSet = backingSet;
        this.toFunction = toFunction;
        this.fromFunction = fromFunction;
    }


    @Override
    public int size() {
        return backingSet.size();
    }

    @Override
    public boolean isEmpty() {
        return backingSet.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        try {
            return backingSet.contains(toFunction.apply((F) o));
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public Iterator<F> iterator() {
        return Iterators.transform(backingSet.iterator(), fromFunction);
    }

    @Override
    public boolean add(F f) {
        return backingSet.add(toFunction.apply(f));
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
    public boolean addAll(Collection<? extends F> c) {
        return false;  //TODO implement this
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;  //TODO implement this
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;  //TODO implement this
    }

    @Override
    public void clear() {
        //TODO implement this
    }
}

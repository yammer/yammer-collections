package com.yammer.collections.guava.azure.transforming;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class TransformingSet<F, T> extends AbstractSet<F> implements Set<F> {
    private Collection<F> collectionWhichIsASet;

    /**
     * This implementation will break if the following is not satisfied:
     * <p/>
     * - for every element F f, fromFunction(toFunction(f)) = f
     * - for every element T f, toFunction(FromFunction(t)) = t
     * <p/>
     * i.e., fromFunction is a bijection and the toFunction is its reverse
     */
    public TransformingSet(Set<T> backingSet, Function<F, T> toFunction, Function<T, F> fromFunction) {
        collectionWhichIsASet = new TransformingCollection<>(backingSet, toFunction, fromFunction);
    }

    @Override
    public int size() {
        return collectionWhichIsASet.size();
    }

    @Override
    public boolean isEmpty() {
        return collectionWhichIsASet.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
            return collectionWhichIsASet.contains(o);
    }

    @Override
    public Iterator<F> iterator() {
        return collectionWhichIsASet.iterator();
    }

    @Override
    public boolean add(F f) {
        return collectionWhichIsASet.add(f);
    }

    @Override
    public boolean remove(Object o) {
        return collectionWhichIsASet.remove(o);
    }

    @Override
    public void clear() {
        collectionWhichIsASet.clear();
    }

    @Override
    public int hashCode() {
        return collectionWhichIsASet.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) { return false; }
        if (o == this) { return true; }
        if (!(o instanceof Set)) {return false;}

        return collectionWhichIsASet.equals(o);
    }
}

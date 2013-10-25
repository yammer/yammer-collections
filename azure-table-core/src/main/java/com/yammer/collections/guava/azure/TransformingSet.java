package com.yammer.collections.guava.azure;

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
    private final Set<T> backingSet;
    private final Function<F, T> toFunction;
    private final Function<T, F> fromFunction;

    /**
     * This implementation will break if the following is not satisfied:
     * <p/>
     * - for every element F f, fromFunction(toFunction(f)) = f
     * - for every element T f, toFunction(FromFunction(t)) = t
     * <p/>
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
        try {
            return backingSet.remove(toFunction.apply((F) o));
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public void clear() {
       backingSet.clear();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder(43, 17);
        for(F f : this) {
            builder.append(f);
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) { return false; }
        if (o == this) { return true; }
        if (!(o instanceof Set)) {return false;}
        Set s = (Set) o;



        Iterator<?> i = s.iterator();
        for(F f : this) {
            if(!i.hasNext() || !f.equals(i.next())) {
                return false;
            }
        }

        return !i.hasNext();
    }
}

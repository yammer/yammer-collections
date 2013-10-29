package com.yammer.collections.guava.azure.transforming;

import com.google.common.base.Function;

import java.util.Set;

public class TransformingSet<F, T> extends TransformingCollection<F, T> implements Set<F> {

    /**
     * This implementation will break if the following is not satisfied:
     * <p/>
     * - for every element F f, fromFunction(toFunction(f)) = f
     * - for every element T f, toFunction(FromFunction(t)) = t
     * <p/>
     * i.e., fromFunction is a bijection and the toFunction is its reverse
     */
    public TransformingSet(Set<T> backingSet, Function<F, T> toFunction, Function<T, F> fromFunction) {
        super(backingSet, toFunction, fromFunction);
    }

    @Override
    public boolean equals(Object o) {
        return o != null && (o == this || o instanceof Set && super.equals(o));

    }
}

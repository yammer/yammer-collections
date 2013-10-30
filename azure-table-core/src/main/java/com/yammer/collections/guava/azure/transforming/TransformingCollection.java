package com.yammer.collections.guava.azure.transforming;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.yammer.collections.guava.azure.transforming.TransformationUtil.safeTransform;

// TODO : change null handling policy to be consistent (remove null handling in other classes)

public class TransformingCollection<F, T> extends AbstractCollection<F> {
    private final Collection<T> backingCollection;
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
    public TransformingCollection(Collection<T> backingCollection, Function<F, T> toFunction, Function<T, F> fromFunction) {
        this.backingCollection = checkNotNull(backingCollection);
        this.toFunction = checkNotNull(toFunction);
        this.fromFunction = checkNotNull(fromFunction);
    }

    @Override
    public int size() {
        return backingCollection.size();
    }

    @Override
    public boolean isEmpty() {
        return backingCollection.isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        try {
            return backingCollection.contains(safeTransform((F) o, toFunction));
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public Iterator<F> iterator() {
        return Iterators.transform(backingCollection.iterator(), fromFunction);
    }

    @Override
    public boolean add(F f) {
        return backingCollection.add(safeTransform(f, toFunction));
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object o) {
        try {
            return backingCollection.remove(safeTransform((F) o, toFunction));
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public void clear() {
        backingCollection.clear();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder(43, 17);
        for (F f : this) {
            builder.append(f);
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof Collection)) {
            return false;
        }
        Collection s = (Collection) o;

        Iterator<?> i = s.iterator();
        for (F f : this) {
            if (!i.hasNext() || !f.equals(i.next())) {
                return false;
            }
        }

        return !i.hasNext();
    }

}

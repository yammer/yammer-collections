package com.yammer.collections.guava.azure;


import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

// TODO make to variants, one that trusts the underyling collection and one that doesn't
public class SetView<E> extends AbstractSet<E> {
    private final CollectionView<E> collectionView;

    private SetView(CollectionView<E> collectionView) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return collectionView.containsAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
       throw new UnsupportedOperationException();
    }

    public static <E> SetView<E> fromSetCollectionView(CollectionView<E> colletionView) {
        return new SetView<>(colletionView);
    }

    public static <E> SetView<E> fromCollectionView(CollectionView<E> colletionView) {
        return new NonSetCollectionBasedSetView<>(colletionView);
    }


    private static class NonSetCollectionBasedSetView<E> extends SetView<E> {
        private final CollectionView<E> collectionView;

        public NonSetCollectionBasedSetView(CollectionView<E> collectionView) {
            super(collectionView);
            this.collectionView = collectionView;
        }

        @Override
        public int size() {
            int size = 0;
            for(E e : this) {
                size++;
            }
            return size;
        }

        @Override
        public Iterator<E> iterator() {
            return new UniequeIterator(collectionView.iterator());
        }
    }

    // this iterator has memory impact (maintains the occurences set) but allows for not loading the full set into memory immidiately
    private static class UniequeIterator<E> implements Iterator<E> {
        private final Iterator<E> baseIterator;
        private final Set<E> occurences;
        private Optional<E> next;

        private UniequeIterator(Iterator<E> baseIterator) {
            this.baseIterator = baseIterator;
            occurences = new HashSet<>();
            next = Optional.absent();
        }

        @Override
        public boolean hasNext() {
            if(!next.isPresent()) {
                next = internalNext();
            }
            return next.isPresent();
        }

        @Override
        public E next() {
            if(!hasNext()) {
                throw new NoSuchElementException();
            }

            E nextElem = next.get();
            next = Optional.absent();
            return nextElem;
        }

        private Optional<E> internalNext() {
            E candiateNext;
            do {
                if(!baseIterator.hasNext()) {
                    return Optional.absent();
                }
                candiateNext = baseIterator.next();
            } while(!occurences.add(candiateNext));

            return Optional.of(candiateNext);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

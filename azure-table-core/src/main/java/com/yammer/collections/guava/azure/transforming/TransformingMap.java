package com.yammer.collections.guava.azure.transforming;


import com.google.common.base.Function;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class TransformingMap<K, V, K1, V1> extends AbstractMap<K, V> {
    private final Map<K1, V1> backingMap;
    private final Function<K, K1> toKeyFunction;
    private final Function<K1, K> fromKeyFunction;
    private final Function<V, V1> toValueFunction;
    private final Function<V1, V> fromValueFunction;

    public TransformingMap(
            Map<K1, V1> backingMap,
            Function<K, K1> toKeyFunction,
            Function<K1, K> fromKeyFunction,
            Function<V, V1> toValueFunction,
            Function<V1, V> fromValueFunction
    ) {
        this.backingMap = backingMap;
        this.toKeyFunction = toKeyFunction;
        this.fromKeyFunction = fromKeyFunction;
        this.toValueFunction = toValueFunction;
        this.fromValueFunction = fromValueFunction;
    }

    @Override
    public boolean isEmpty() {
        return backingMap.isEmpty();
    }

    @Override
    public int size() {
        return backingMap.size();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;  //TODO implement this
    }

    @Override
    public boolean containsValue(Object o) {
        try {
            return backingMap.containsValue(toValueFunction.apply((V) o));
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public boolean containsKey(Object key) {
        try {
            return backingMap.containsKey(toKeyFunction.apply((K) key));
        } catch (ClassCastException e) {
            return false;
        }
    }

    public V get(Object key) {
        try {
           V1 retValue = backingMap.get(toKeyFunction.apply((K) key));
           return retValue != null? fromValueFunction.apply(retValue):null;
        } catch (ClassCastException e) {
            return null;
        }
    }

    public V put(K key, V value) {
        V1 retValue = backingMap.put(toKeyFunction.apply(key), toValueFunction.apply(value));
        return retValue != null? fromValueFunction.apply(retValue):null;
    }

    public V remove(Object key) {
        try {
            V1 retValue = backingMap.remove(toKeyFunction.apply((K) key));
            return retValue != null? fromValueFunction.apply(retValue):null;
        } catch (ClassCastException e) {
            return null;
        }
    }

    public void clear() {
        backingMap.clear();
    }

    public Set<K> keySet() {
        return new TransformingSet<>(
                backingMap.keySet(), toKeyFunction, fromKeyFunction
        );
    }

    public Collection<V> values() {
        return new TransformingCollection<>(
            backingMap.values(), toValueFunction, fromValueFunction
        );
    }

    public static class TransformingEntry<K, V, K1, V1> implements Entry<K, V> {
        private final Entry<K1, V1> backingEntry;
        private final Function<K, K1> toKeyFunction;
        private final Function<K1, K> fromKeyFunction;
        private final Function<V, V1> toValueFunction;
        private final Function<V1, V> fromValueFunction;

        public TransformingEntry(Entry<K1, V1> backingEntry,
                                 Function<K, K1> toKeyFunction,
                                 Function<K1, K> fromKeyFunction,
                                 Function<V, V1> toValueFunction,
                                 Function<V1, V> fromValueFunction) {
            this.backingEntry = backingEntry;
            this.toKeyFunction = toKeyFunction;
            this.fromKeyFunction = fromKeyFunction;
            this.toValueFunction = toValueFunction;
            this.fromValueFunction = fromValueFunction;
        }

        @Override
        public K getKey() {
            return null;  //TODO implement this
        }

        @Override
        public V getValue() {
            return null;  //TODO implement this
        }

        @Override
        public V setValue(V value) {
            return null;  //TODO implement this
        }
    }
}

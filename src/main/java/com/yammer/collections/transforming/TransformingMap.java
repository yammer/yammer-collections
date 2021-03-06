/**
 * Copyright (c) Microsoft Corporation
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER
 * EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS
 * OF TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing permissions and limitations under
 * the License.
 */
package com.yammer.collections.transforming;


import com.google.common.base.Function;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.yammer.collections.transforming.TransformationUtil.safeTransform;

public class TransformingMap<K, V, K1, V1> extends AbstractMap<K, V> {
    private final Map<K1, V1> backingMap;
    private final Function<K, K1> toKeyFunction;
    private final Function<K1, K> fromKeyFunction;
    private final Function<V, V1> toValueFunction;
    private final Function<V1, V> fromValueFunction;
    private final Function<Entry<K, V>, Entry<K1, V1>> toEntryFunction;
    private final Function<Entry<K1, V1>, Entry<K, V>> fromEntryFunction;

    private TransformingMap(
            Map<K1, V1> backingMap,
            final Function<K, K1> toKeyFunction,
            final Function<K1, K> fromKeyFunction,
            final Function<V, V1> toValueFunction,
            final Function<V1, V> fromValueFunction
    ) {
        this.backingMap = checkNotNull(backingMap);
        this.toKeyFunction = checkNotNull(toKeyFunction);
        this.fromKeyFunction = checkNotNull(fromKeyFunction);
        this.toValueFunction = checkNotNull(toValueFunction);
        this.fromValueFunction = checkNotNull(fromValueFunction);
        toEntryFunction = new Function<Entry<K, V>, Entry<K1, V1>>() {
            @Override
            public Entry<K1, V1> apply(Entry<K, V> kvEntry) {
                return new TransformingEntry<K1, V1, K, V>(
                        kvEntry,
                        toKeyFunction,
                        fromValueFunction,
                        toValueFunction
                );
            }
        };
        fromEntryFunction = new

                Function<Entry<K1, V1>, Entry<K, V>>() {
                    @Override
                    public Entry<K, V> apply(Entry<K1, V1> kvEntry) {
                        return new TransformingEntry<K, V, K1, V1>(
                                kvEntry,
                                fromKeyFunction,
                                toValueFunction,
                                fromValueFunction
                        );
                    }
                };
    }

    public static <K, V, K1, V1> Map<K, V> create(
            Map<K1, V1> backingMap,
            Function<K, K1> toKeyFunction,
            Function<K1, K> fromKeyFunction,
            Function<V, V1> toValueFunction,
            Function<V1, V> fromValueFunction
    ) {
        return new TransformingMap<K, V, K1, V1>(backingMap, toKeyFunction, fromKeyFunction, toValueFunction, fromValueFunction);
    }

    @Override
    public boolean isEmpty() {
        return backingMap.isEmpty();
    }

    @Override
    public int size() {
        return backingMap.size();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Set<Entry<K, V>> entrySet() {
        return TransformingSet.create(
                backingMap.entrySet(),
                toEntryFunction,
                fromEntryFunction
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsValue(Object o) {
        try {
            return o != null &&
                    backingMap.containsValue(safeTransform((V) o, toValueFunction));
        } catch (ClassCastException ignored) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(Object key) {
        try {
            return key != null &&
                    backingMap.containsKey(safeTransform((K) key, toKeyFunction));
        } catch (ClassCastException ignored) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key) {
        try {
            return key == null ? null :
                    safeTransform(
                            backingMap.get(safeTransform((K) key, toKeyFunction)),
                            fromValueFunction
                    );
        } catch (ClassCastException ignored) {
            return null;
        }
    }

    @Override
    public V put(K key, V value) {
        K1 tKey = safeTransform(checkNotNull(key), toKeyFunction);
        V1 tValue = safeTransform(checkNotNull(value), toValueFunction);
        return safeTransform(
                backingMap.put(tKey, tValue),
                fromValueFunction
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        try {
            return key == null ? null :
                    safeTransform(
                            backingMap.remove(safeTransform((K) key, toKeyFunction)),
                            fromValueFunction
                    );
        } catch (ClassCastException ignored) {
            return null;
        }
    }

    @Override
    public void clear() {
        backingMap.clear();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Set<K> keySet() {
        return TransformingSet.create(
                backingMap.keySet(), toKeyFunction, fromKeyFunction
        );
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Collection<V> values() {
        return TransformingCollection.create(
                backingMap.values(), toValueFunction, fromValueFunction
        );
    }

    private static class TransformingEntry<K, V, K1, V1> implements Entry<K, V> {
        private final Entry<K1, V1> backingEntry;
        private final Function<K1, K> fromKeyFunction;
        private final Function<V, V1> toValueFunction;
        private final Function<V1, V> fromValueFunction;

        public TransformingEntry(Entry<K1, V1> backingEntry,
                                 Function<K1, K> fromKeyFunction,
                                 Function<V, V1> toValueFunction,
                                 Function<V1, V> fromValueFunction) {
            this.backingEntry = backingEntry;
            this.fromKeyFunction = fromKeyFunction;
            this.toValueFunction = toValueFunction;
            this.fromValueFunction = fromValueFunction;
        }

        @Override
        public K getKey() {
            return safeTransform(backingEntry.getKey(), fromKeyFunction);
        }

        @Override
        public V getValue() {
            return safeTransform(backingEntry.getValue(), fromValueFunction);
        }

        @Override
        public V setValue(V value) {
            return safeTransform(
                    backingEntry.setValue(safeTransform(checkNotNull(value), toValueFunction)),
                    fromValueFunction
            );
        }
    }
}

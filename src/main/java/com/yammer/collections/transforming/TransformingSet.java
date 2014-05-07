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

import java.util.Set;

/**
 * This implementation will break if the following is not satisfied:
 * <p/>
 * - for every element F f, fromFunction(toFunction(f)) = f
 * - for every element T f, toFunction(FromFunction(t)) = t
 * <p/>
 * i.e., fromFunction is a bijection and the toFunction is its reverse
 *
 * Does not support null values, i.e., contains(null) returns false, add(null) throws a NullPointerException
 */
public class TransformingSet<F, T> extends TransformingCollection<F, T> implements Set<F> {

    public static <F,T> Set<F> create(
            Set<T> backingCollection,
            Function<F, T> toFunction,
            Function<T, F> fromFunction
    ) {
        return new TransformingSet<F,T>(backingCollection, toFunction, fromFunction);
    }


    private TransformingSet(Set<T> backingSet, Function<F, T> toFunction, Function<T, F> fromFunction) {
        super(backingSet, toFunction, fromFunction);
    }

    @Override
    public boolean equals(Object o) {
        return o != null && (o == this || o instanceof Set && super.equals(o));
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}

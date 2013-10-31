package com.yammer.collections.guava.azure.transforming;

import com.google.common.base.Function;

final class TransformationUtil {
    private TransformationUtil() {}


    static <F, T> T safeTransform(F from, Function<F, T> conversionFunction) {
        return from == null ? null : conversionFunction.apply(from);
    }

}

package com.yammer.collections.transforming;

import com.google.common.base.Function;

public class TypedFunction<F, T> implements Function<Object, Object> {

    public static <F, T> Function<Object, Object> wrap(Function<F, T> delegate) {
        return new TypedFunction<F, T>(delegate);
    }

    private final Function<F, T> delegate;

    private TypedFunction(Function<F, T> delegate) {
        this.delegate = delegate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object apply(Object input) {
        return delegate.apply((F) input);
    }
}

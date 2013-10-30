package com.yammer.collections.guava.azure.serialisation.json;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Table;
import com.yammer.collections.guava.azure.transforming.TransformingTable;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

// TODO extends? or compose, make classes final and factory method created (transforming classes)
public class JsonSerializingTable<R, C, V> extends TransformingTable<R, C, V, String, String, String> {

    public JsonSerializingTable(
            Table<String, String, String> backingTable,
            Class<R> rowClass, Class<C> columnClass, Class<V> valueClass) {
        super(backingTable,
                JsonSerializingTable.<R>createSerializationFunction(),
                JsonSerializingTable.<R>createDeserializationFunction(rowClass),
                JsonSerializingTable.<C>createSerializationFunction(),
                JsonSerializingTable.<C>createDeserializationFunction(columnClass),
                JsonSerializingTable.<V>createSerializationFunction(),
                JsonSerializingTable.<V>createDeserializationFunction(valueClass)
        );
    }

    private static <F> Function<F, String> createSerializationFunction() {
        return new Function<F, String>() {
            @Override
            public String apply(F f) {
                try {
                    ObjectMapper om = new ObjectMapper(); // TODO can we have a shared one?
                    return om.writeValueAsString(f);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    private static <T> Function<String, T> createDeserializationFunction(final Class<T> klass) {
        return new Function<String, T>() {
            @Override
            public T apply(String t) {
                try {
                    ObjectMapper om = new ObjectMapper(); // TODO can we have a shared one?
                    return om.readValue(t, klass);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

}

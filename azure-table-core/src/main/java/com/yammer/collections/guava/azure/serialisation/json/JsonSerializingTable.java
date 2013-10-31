package com.yammer.collections.guava.azure.serialisation.json;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Table;
import com.yammer.collections.guava.azure.transforming.TransformingTable;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class JsonSerializingTable<R, C, V> extends TransformingTable<R, C, V, String, String, String> {

    public JsonSerializingTable(
            Table<String, String, String> backingTable,
            Class<R> rowClass, Class<C> columnClass, Class<V> valueClass) {
        this(backingTable, rowClass, columnClass, valueClass, new ObjectMapper());
    }

    public JsonSerializingTable(
            Table<String, String, String> backingTable,
            Class<R> rowClass, Class<C> columnClass, Class<V> valueClass,
            ObjectMapper objectMapper) {
        super(backingTable,
                JsonSerializingTable.<R>createSerializationFunction(objectMapper),
                JsonSerializingTable.<R>createDeserializationFunction(objectMapper, rowClass),
                JsonSerializingTable.<C>createSerializationFunction(objectMapper),
                JsonSerializingTable.<C>createDeserializationFunction(objectMapper, columnClass),
                JsonSerializingTable.<V>createSerializationFunction(objectMapper),
                JsonSerializingTable.<V>createDeserializationFunction(objectMapper, valueClass)
        );

    }

    private static <F> Function<F, String> createSerializationFunction(final ObjectMapper om) {
        return new Function<F, String>() {
            @SuppressWarnings("OverlyBroadCatchBlock")
            @Override
            public String apply(F f) {
                try {
                    return om.writeValueAsString(f);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    private static <T> Function<String, T> createDeserializationFunction(final ObjectMapper om, final Class<T> klass) {
        return new Function<String, T>() {
            @SuppressWarnings("OverlyBroadCatchBlock")
            @Override
            public T apply(String t) {
                try {
                    return om.readValue(t, klass);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

}

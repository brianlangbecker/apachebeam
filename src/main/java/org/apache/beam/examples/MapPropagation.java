package org.apache.beam.examples;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import javax.annotation.Nullable;
import java.util.Map;

public class MapPropagation<T> implements TextMapSetter<Map<String, String>>, TextMapGetter<Map<String, String>> {

    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
        return carrier.keySet();
    }

    @Nullable
    @Override
    public String get(@Nullable Map<String, String> carrier, String key) {
        return carrier.get(key);
    }

    @Override
    public void set(@Nullable Map<String, String> carrier, String key, String value) {
        carrier.put(key, value);
    }
}

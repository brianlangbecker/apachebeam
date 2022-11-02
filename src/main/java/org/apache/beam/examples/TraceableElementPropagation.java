package org.apache.beam.examples;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TraceableElementPropagation<T> implements TextMapSetter<TraceableElement<T>>, TextMapGetter<TraceableElement<T>> {

    @Override
    public Iterable<String> keys(TraceableElement<T> tTraceableElement) {
        return tTraceableElement.getContext().keySet();
    }

    @Nullable
    @Override
    public String get(@Nullable TraceableElement<T> tTraceableElement, String s) {
        return tTraceableElement.getContext().get(s);
    }

    @Override
    public void set(@Nullable TraceableElement<T> tTraceableElement, String s, String s1) {
        tTraceableElement.getContext().put(s, s1);
    }
}

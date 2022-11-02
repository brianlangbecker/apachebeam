package org.apache.beam.examples;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.*;

@EqualsAndHashCode
@ToString
@Getter
public class TraceableElement<T> implements Serializable{
    private final T element;
    private final Map<String, String> context;
    private final List<Map<String, String>> linkedContext;

    public TraceableElement(final T element) {
        this(element, new HashMap<>());
    }

    public TraceableElement(final T element, Map<String, String> context) {
        this(element, context, Collections.emptyList());
    }

    public TraceableElement(final T element, Map<String, String> context, List<Map<String, String>> linkedContext) {
        this.element = element;
        this.context = context;
        this.linkedContext = linkedContext;
    }
}

package org.apache.beam.examples;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.sdk.common.*;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
public abstract class TrackableDoFn<InputT, OutputT> extends DoFn<TraceableElement<InputT>, TraceableElement<OutputT>> {
    private final OpenTelemetryOptions openTelemetryOptions;

    protected TrackableDoFn(OpenTelemetryOptions openTelemetryOptions) {
        this.openTelemetryOptions = openTelemetryOptions;
    }

    protected void customSetup() {
        // override with custom setup
    }
    abstract List<OutputT> process(final InputT inputT);
    protected Map<AttributeKey, Object> getAttributes() {
        return Collections.emptyMap();
    }

    protected SpanBuilder getSpanBuilder(final String spanName) {
        return getTracer().spanBuilder(spanName);
    }

    private transient Tracer tracer;
    private transient OpenTelemetry openTelemetry;

    protected OpenTelemetry getOpenTelemetry() {
        if (openTelemetry == null) {
            // todo pull in options from pipeline options
            openTelemetry = ExampleConfiguration.initOpenTelemetry(openTelemetryOptions, getClass().getSimpleName());
        }
        return openTelemetry;
    }
    protected Tracer getTracer() {
        if (tracer == null) {
            // todo pull in instrumentation name from pipeline options
            tracer = getOpenTelemetry().getTracer("brian.test");
        }
        return tracer;
    }

    @Setup
    public void setup() {
        customSetup();
        getTracer();
    }

    @ProcessElement
    public void processElement(@Element TraceableElement<InputT> traceableElement, OutputReceiver<TraceableElement<OutputT>> receiver) {
        final Context parentContext = getOpenTelemetry().getPropagators().getTextMapPropagator().extract(Context.current(), traceableElement, new TraceableElementPropagation<>());
        final SpanBuilder spanBuilder = getTracer().spanBuilder(getClass().getName()).setSpanKind(SpanKind.SERVER);
        traceableElement.getLinkedContext().stream().forEach(linked -> {
            final Context context = getOpenTelemetry().getPropagators().getTextMapPropagator().extract(Context.current(), linked, new MapPropagation<>());
            spanBuilder.addLink(Span.fromContext(context).getSpanContext());
        });
        if (parentContext != null) {
            spanBuilder.setParent(parentContext);
        }
        getAttributes().forEach(spanBuilder::setAttribute);
        final Span span = spanBuilder.startSpan();
        try (Scope scope = span.makeCurrent()) {
            process(traceableElement.getElement()).forEach(output -> {
                span.setAttribute("output",output.toString());
                final TraceableElement<OutputT> traceableOutput = new TraceableElement<>(output);
                getOpenTelemetry().getPropagators().getTextMapPropagator().inject(Context.current(), traceableOutput, new TraceableElementPropagation<>());
                receiver.output(traceableOutput);
            });
        } catch (Throwable t) {
            span.setStatus(StatusCode.ERROR, t.getMessage());
        } finally {
            span.end();
        }
    }
}

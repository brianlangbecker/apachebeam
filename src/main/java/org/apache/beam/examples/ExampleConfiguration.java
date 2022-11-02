package org.apache.beam.examples;

import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.*;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import lombok.SneakyThrows;

import java.util.concurrent.TimeUnit;

/**
 * All SDK management takes place here, away from the instrumentation code, which should only access
 * the OpenTelemetry APIs.
 */
class ExampleConfiguration {

    /**
     * Initialize an OpenTelemetry SDK with a Jaeger exporter and a SimpleSpanProcessor.
     *
     * @param jaegerHost The host of your Jaeger instance.
     * @param jaegerPort the port of your Jaeger instance.
     * @return A ready-to-use {@link OpenTelemetry} instance.
     */
    @SneakyThrows
    static OpenTelemetry initOpenTelemetry(final OpenTelemetryOptions openTelemetryOptions, final String serviceName) {
        final SpanExporter spanExporter;
        if (openTelemetryOptions.isUseLocal()) {
            // Create a channel towards Jaeger end point
            ManagedChannel jaegerChannel =
                    ManagedChannelBuilder.forAddress(
                            openTelemetryOptions.getJaegerHost(),
                            openTelemetryOptions.getJaegerPort()).usePlaintext().build();
            // Export traces to Jaeger
            spanExporter =
                    JaegerGrpcSpanExporter.builder()
                            .setChannel(jaegerChannel)
                            .setTimeout(30, TimeUnit.SECONDS)
                            .build();
        } else {
            // Export traces to GCP Trace
            spanExporter = TraceExporter.createWithConfiguration(
                    TraceConfiguration.builder().setProjectId(openTelemetryOptions.getGcpProjectId()).build());
        }

        Resource serviceNameResource =
                Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, serviceName));

        SdkTracerProvider tracerProvider =
                SdkTracerProvider.builder()
                        .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                        .setResource(Resource.getDefault().merge(serviceNameResource))
                        .setSampler(Sampler.alwaysOn())
                        .build();
        OpenTelemetrySdk openTelemetry =
                OpenTelemetrySdk.builder().setTracerProvider(tracerProvider)
                        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance())).build();

        // it's always a good idea to shut down the SDK cleanly at JVM exit.
        Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::close));

        return openTelemetry;
    }
}
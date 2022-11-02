package org.apache.beam.examples;

import lombok.*;

import java.io.Serializable;

@AllArgsConstructor
@Builder
@EqualsAndHashCode
@ToString
@Getter
public class OpenTelemetryOptions implements Serializable {
    private final String jaegerHost;
    private final int jaegerPort;
    private final String gcpProjectId;
    private final boolean useLocal;
}

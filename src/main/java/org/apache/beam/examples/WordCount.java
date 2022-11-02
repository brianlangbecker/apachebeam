/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An example that counts words in Shakespeare and includes Beam best practices.
 *
 * <p>
 * This class, {@link WordCount}, is the second in a series of four successively
 * more detailed
 * 'word count' examples. You may first want to take a look at
 * {@link MinimalWordCount}. After
 * you've looked at this example, then see the {@link DebuggingWordCount}
 * pipeline, for introduction
 * of additional concepts.
 *
 * <p>
 * For a detailed walkthrough of this example, see <a
 * href="https://beam.apache.org/get-started/wordcount-example/">
 * https://beam.apache.org/get-started/wordcount-example/ </a>
 *
 * <p>
 * Basic concepts, also in the MinimalWordCount example: Reading text files;
 * counting a
 * PCollection; writing to text files
 *
 * <p>
 * New Concepts:
 *
 * <pre>
 *   1. Executing a Pipeline both locally and using the selected runner
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Building a composite transform
 *   4. Defining your own pipeline options
 * </pre>
 *
 * <p>
 * Concept #1: you can execute this pipeline either locally or using by
 * selecting another runner.
 * These are now command-line options and not hard-coded as they were in the
 * MinimalWordCount
 * example.
 *
 * <p>
 * To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * <p>
 * To execute this pipeline, specify a local output file (if using the
 * {@code DirectRunner}) or
 * output prefix on a supported distributed file system.
 *
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>
 * The input file defaults to a public data set containing the text of of King
 * Lear, by William
 * Shakespeare. You can override it and choose your own input with
 * {@code --inputFile}.
 */
public class WordCount {

  /**
   * Concept #2: You can make your pipeline assembly code less verbose by defining
   * your DoFns
   * statically out-of-line. This DoFn tokenizes lines of text into individual
   * words; we pass it to
   * a ParDo in the pipeline.
   */
  static class ExtractWordsFn extends TrackableDoFn<String, String> {
    private static final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private static final Distribution lineLenDist = Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

    protected ExtractWordsFn(OpenTelemetryOptions openTelemetryOptions) {

      super(openTelemetryOptions);
    }

    @Override
    List<String> process(String element) {

      lineLenDist.update(element.length());
      if (element.trim().isEmpty()) {
        emptyLines.inc();
      }
      // Split the line into words.
      String[] words = element.split(ExampleUtils.TOKENIZER_PATTERN, -1);

      // Output each word encountered into the output PCollection.
      return Arrays.asList(words);
    }
  }

  /** A SimpleFunction that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn extends TrackableDoFn<String, String> {

    protected FormatAsTextFn(OpenTelemetryOptions openTelemetryOptions) {
      super(openTelemetryOptions);
    }

    @Override
    List<String> process(String s) {
      return Arrays.asList(s);
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a
   * PCollection of
   * formatted word counts.
   *
   * <p>
   * Concept #3: This is a custom composite transform that bundles two transforms
   * (ParDo and
   * Count) as a reusable PTransform subclass. Using composite transforms allows
   * for easy reuse,
   * modular testing, and an improved monitoring experience.
   */
  @AllArgsConstructor
  public static class CountWords
      extends PTransform<PCollection<String>, PCollection<TraceableElement<String>>> {
    private final OpenTelemetryOptions openTelemetryOptions;

    @Override
    public PCollection<TraceableElement<String>> expand(PCollection<String> lines) {
      PCollection<TraceableElement<String>> words = lines.apply("Add tracing", TrackableConversion.create())
          .setCoder(TraceableCoder.of(StringUtf8Coder.of()))
          .apply(ParDo.of(new ExtractWordsFn(openTelemetryOptions)));

      return words;
    }
  }

  @NoArgsConstructor(staticName = "create")
  public static class TrackableConversion<T> extends PTransform<PCollection<T>, PCollection<TraceableElement<T>>> {
    @Override
    public PCollection<TraceableElement<T>> expand(PCollection<T> input) {

      return input.apply(MapElements.into(new TypeDescriptor<TraceableElement<T>>() {
      }).via(TraceableElement::new));
    }
  }

  public static class RemoveTracking<T> extends PTransform<PCollection<TraceableElement<T>>, PCollection<T>> {

    @Override
    public PCollection<T> expand(PCollection<TraceableElement<T>> input) {
      return input.apply(MapElements.into(new TypeDescriptor<T>() {
      }).via(TraceableElement::getElement));
    }
  }

  @AllArgsConstructor(staticName = "of")
  @Getter
  public static class TraceableCoder<T> extends CustomCoder<TraceableElement<T>> {
    private static final MapCoder<String, String> MAP_CODER = MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    private final Coder<T> elementCoder;

    @Override
    public void encode(TraceableElement<T> value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
      elementCoder.encode(value.getElement(), outStream);
      MAP_CODER.encode(value.getContext(), outStream);
      ListCoder.of(MAP_CODER).encode(value.getLinkedContext(), outStream);
    }

    @Override
    public TraceableElement<T> decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
      final T element = elementCoder.decode(inStream);
      final Map<String, String> context = MAP_CODER.decode(inStream);
      final List<Map<String, String>> linkedContext = ListCoder.of(MAP_CODER).decode(inStream);
      return new TraceableElement<>(element, context, linkedContext);
    }
  }

  public static class CountCombineFn extends Combine.CombineFn<String, Integer, Integer> {

    @Override
    public Integer createAccumulator() {
      return 0;
    }

    @Override
    public Integer addInput(Integer mutableAccumulator, String input) {
      return mutableAccumulator + 1;
    }

    @Override
    public Integer mergeAccumulators(@UnknownKeyFor @NonNull @Initialized Iterable<Integer> accumulators) {
      Integer total = 0;
      for (Integer val : accumulators) {
        total += val;
      }
      return total;
    }

    @Override
    public Integer extractOutput(Integer accumulator) {
      return accumulator;
    }
  }

  /**
   * Options supported by {@link WordCount}.
   *
   * <p>
   * Concept #4: Defining your own configuration options. Here, you can add your
   * own arguments to
   * be processed by the command-line parser, and specify default values for them.
   * You can then
   * access the options values in your pipeline code.
   *
   * <p>
   * Inherits standard configuration options.
   */
  public interface WordCountOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of
     * King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);

    @Default.Boolean(true)
    Boolean getUseLocal();

    void setUseLocal(Boolean value);
  }

  public static class ProcessCombineResultsFn extends TrackableDoFn<Integer, String> {

    protected ProcessCombineResultsFn(OpenTelemetryOptions openTelemetryOptions) {
      super(openTelemetryOptions);
    }

    @Override
    List<String> process(Integer integer) {
      return Collections.singletonList(integer.toString());
    }
  }

  static void runWordCount(WordCountOptions options) {
    Pipeline p = Pipeline.create(options);

    final OpenTelemetryOptions openTelemetryOptions = OpenTelemetryOptions.builder()
        .jaegerHost("localhost")
        .jaegerPort(14250)
        .gcpProjectId("trace-prototyping-1")
        .useLocal(options.getUseLocal())
        .build();

    // final SpanBuilder spanBuilder =
    // ExampleConfiguration.initOpenTelemetry(openTelemetryOptions, "Start
    // Run").getTracer("tim.test").spanBuilder("start").setSpanKind(SpanKind.SERVER);

    // final Span span = spanBuilder.startSpan();
    // span.makeCurrent();

    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform,
    // and passes the
    // static FormatAsTextFn() to the ParDo transform.
    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
        .apply(new CountWords(openTelemetryOptions))
        .apply(ParDo.of(new FormatAsTextFn(openTelemetryOptions)))
        .apply(WithKeys.of(TraceableElement::getElement))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), TraceableCoder.of(StringUtf8Coder.of())))
        .apply(Combine.perKey(TrackableCombineFn.of(new CountCombineFn())))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), TraceableCoder.of(VarIntCoder.of())))
        .apply(Values.create())
        .apply("process", ParDo.of(new ProcessCombineResultsFn(openTelemetryOptions)))
        .apply(new RemoveTracking<>())
        .setCoder(StringUtf8Coder.of())
        .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();
    // span.end();
  }

  public static void main(String[] args) {
    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

    runWordCount(options);
  }
}

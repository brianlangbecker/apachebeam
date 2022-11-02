package org.apache.beam.examples;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.Combine;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@AllArgsConstructor(staticName = "of")
public class TrackableCombineFn<InputT, AccumT, OutputT> extends Combine.CombineFn<TraceableElement<InputT>, TrackableCombineFn.TraceableAccumulator<AccumT>, TraceableElement<OutputT>> {
    private final Combine.CombineFn<InputT, AccumT, OutputT> innerCombineFn;

    @Override
    public TraceableAccumulator<AccumT> createAccumulator() {
        return TraceableAccumulator.of(innerCombineFn.createAccumulator(), new ArrayList<>());
    }

    @Override
    public TraceableAccumulator<AccumT> addInput(TraceableAccumulator<AccumT> mutableAccumulator, TraceableElement<InputT> input) {
        final AccumT acumm = innerCombineFn.addInput(mutableAccumulator.getAccum(), input.getElement());
        final List<Map<String, String>> contexts = mutableAccumulator.getContexts();
        contexts.add(input.getContext());
        return TraceableAccumulator.of(acumm, contexts);
    }

    @Override
    public TraceableAccumulator<AccumT> mergeAccumulators(@UnknownKeyFor @NonNull @Initialized Iterable<TraceableAccumulator<AccumT>> accumulators) {
        final List<Map<String, String>> contexts = new ArrayList<>();
        final AccumT accum = innerCombineFn.mergeAccumulators(StreamSupport.stream(accumulators.spliterator(), false).map(traceableAccumulator -> {
            contexts.addAll(traceableAccumulator.getContexts());
            return traceableAccumulator.getAccum();
        }).collect(Collectors.toList()));
        return TraceableAccumulator.of(accum, contexts);
    }

    @Override
    public TraceableElement<OutputT> extractOutput(TraceableAccumulator<AccumT> accumulator) {
        final OutputT outputT = innerCombineFn.extractOutput(accumulator.getAccum());
        return new TraceableElement<>(outputT, accumulator.getContexts().get(0), accumulator.getContexts().subList(1, accumulator.getContexts().size()));
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Coder<TraceableAccumulator<AccumT>> getAccumulatorCoder(@UnknownKeyFor @NonNull @Initialized CoderRegistry registry, @UnknownKeyFor @NonNull @Initialized Coder<TraceableElement<InputT>> inputCoder) throws @UnknownKeyFor@NonNull@Initialized CannotProvideCoderException {
        final WordCount.TraceableCoder<InputT> coder = (WordCount.TraceableCoder<InputT>) inputCoder;
        final Coder<AccumT> accumTCoder = innerCombineFn.getAccumulatorCoder(registry, coder.getElementCoder());
        return TraceableAccumulatorCoder.of(accumTCoder);
    }

    @AllArgsConstructor(staticName = "of")
    @Getter
    @EqualsAndHashCode
    @ToString
    static class TraceableAccumulator<AccumT> {
        private final AccumT accum;
        private final List<Map<String, String>> contexts;
    }

    @AllArgsConstructor(staticName = "of")
    static class TraceableAccumulatorCoder<AccumT> extends CustomCoder<TraceableAccumulator<AccumT>> {
        private static final Coder<List<Map<String, String>>> CONTEXTS_CODER = ListCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
        private final Coder<AccumT> accumTCoder;

        @Override
        public void encode(TraceableAccumulator<AccumT> value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            accumTCoder.encode(value.getAccum(), outStream);
            CONTEXTS_CODER.encode(value.getContexts(), outStream);
        }

        @Override
        public TraceableAccumulator<AccumT> decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            final AccumT accum = accumTCoder.decode(inStream);
            final List<Map<String, String>> contexts = CONTEXTS_CODER.decode(inStream);
            return new TraceableAccumulator<>(accum, contexts);
        }
    }
}

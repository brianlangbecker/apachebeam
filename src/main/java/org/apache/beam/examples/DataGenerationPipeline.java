package org.apache.beam.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataGenerationPipeline {

    public interface DataGenerationPipelineOptions extends PipelineOptions {

        String getOutput();

        void setOutput(String value);

        int getCount();

        void setCount(int count);
    }

    static void runWordCount(DataGenerationPipelineOptions options) {
        Pipeline p = Pipeline.create(options);

        List<Integer> inputs = new ArrayList<>();
        for (int i = 0; i < options.getCount(); i++) {
            inputs.add(i);
        }
        p.apply("num entries", Create.of(inputs))
                .apply("Generate Data", ParDo.of(new JsonBuilder()))
                .apply("Write Data", TextIO.write().to(options.getOutput()).withSuffix(".jsonl"));

        p.run().waitUntilFinish();
    }

    public static class JsonBuilder extends DoFn<Integer, String> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @ProcessElement
        public void processElement(@Element final Integer element, final OutputReceiver<String> receiver) throws IOException {
            for (int i = 0; i < 100; i++) {
                receiver.output(OBJECT_MAPPER.writeValueAsString(BasicTestObj.randomInstance()));
            }
        }
    }

    public static void main(String[] args) {
        DataGenerationPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DataGenerationPipelineOptions.class);

        runWordCount(options);
    }
}

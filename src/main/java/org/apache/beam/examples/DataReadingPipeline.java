package org.apache.beam.examples;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.fasterxml.jackson.module.blackbird.BlackbirdModule;
import com.google.errorprone.annotations.Var;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import lombok.NoArgsConstructor;
import org.apache.beam.examples.avro.NestedObj;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.junit.Test;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

public class DataReadingPipeline {

    public interface DataReadingPipelineOptions extends PipelineOptions {

        String getInput();

        void setInput(String value);

        String getCoder();

        void setCoder(String value);

        boolean getShuffle();

        void setShuffle(boolean value);
    }

    static void runWordCount(DataReadingPipelineOptions options) throws Exception {
        Pipeline p = Pipeline.create(options);

        final Coder<BasicTestObj> coder;
        switch (options.getCoder()) {
            case "java":
                coder = SerializableCoder.of(BasicTestObj.class);
                break;
            case "fst":
                coder = FstCoder.of(BasicTestObj.class);
                break;
            case "kryo":
                coder = KryoCoder.of(kyro -> kyro.register(BasicTestObj.class));
                break;
            case "json":
                coder = new JsonCoder<>(BasicTestObj.class);
                break;
            case "delegate":
                coder = JsonDelegateCoder.of(BasicTestObj.class);
                break;
            case "hand":
                coder = new HandCoder2();
                break;
            case "schema":
//                Schema schema = Schema.builder().addInt32Field("testInt").addNullableField("testWrappedInt", Schema.FieldType.INT32).addInt64Field("testLong").addNullableField("testWrappedLong", Schema.FieldType.INT64).addDoubleField("testDouble").addNullableField("testWrappedDouble", Schema.FieldType.DOUBLE).addStringField("testString").build();
//                p.getSchemaRegistry().registerSchemaForClass(BasicTestObj.class, schema,
//                        input -> Row.withSchema(schema).addValues(input.getTestInt(), input.getTestWrappedInt(), input.getTestLong(), input.getTestWrappedLong(), input.getTestDouble(), input.getTestWrappedDouble(), input.getTestString()).build(),
//                        row -> new BasicTestObj(row.getInt32(0), row.getInt32(1), row.getInt64(2), row.getInt64(3), row.getDouble(4), row.getDouble(5), row.getString(6)));
                coder = p.getSchemaRegistry().getSchemaCoder(BasicTestObj.class);
                break;
            case "proto":
            case "avro":
                coder = null;
                break;
            default:
                throw new RuntimeException("Unknown coder input");
        }


        if (coder != null) {
            PCollection<KV<String, BasicTestObj>> tmp = p.apply("ReadLines", TextIO.read().from(options.getInput()))
                    .apply("To Java Objects", ParDo.of(new ReadFromJson()))
                    .setCoder(coder)
                    .apply("Add Random Keys", WithKeys.of(input -> Integer.toString(input.hashCode())))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), coder));

            if (options.getShuffle()) {
                tmp = tmp.apply("Reshuffle", Reshuffle.of());
            }

            tmp.apply("Drop keys", Values.create());
        } else if (options.getCoder().equals("proto")) {

            PCollection<KV<String, BasicTestObjOuterClass.BasicTestObj>> tmp = p.apply("ReadLines", TextIO.read().from(options.getInput()))
                    .apply("To Java Objects", ParDo.of(new ReadFromJson()))
                    .apply("conver to proto", ParDo.of(new ProtoConvert2()))
                    .setCoder(ProtoCoder.of(BasicTestObjOuterClass.BasicTestObj.class))
                    .apply("Add Random Keys", WithKeys.of(input -> Integer.toString(input.hashCode())))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), ProtoCoder.of(BasicTestObjOuterClass.BasicTestObj.class)));
            if (options.getShuffle()) {
               tmp = tmp.apply("Reshuffle", Reshuffle.of());
            }
            tmp.apply("Drop Keys", Values.create());
        } else if (options.getCoder().equals("avro")) {
            PCollection<KV<String, org.apache.beam.examples.avro.BasicTestObj>> tmp = p.apply("ReadLines", TextIO.read().from(options.getInput()))
                    .apply("To Java Objects", ParDo.of(new ReadFromJson()))
                    .apply("conver to avro", ParDo.of(new AvroConvert2()))
                    .setCoder(AvroCoder.of(org.apache.beam.examples.avro.BasicTestObj.class))
                    .apply("Add Random Keys", WithKeys.of(input -> Integer.toString(input.hashCode())))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(org.apache.beam.examples.avro.BasicTestObj.class)));
            if (options.getShuffle()) {
                tmp = tmp.apply("Reshuffle", Reshuffle.of());
            }
            tmp.apply("Drop Keys", Values.create());
        }

        p.run().waitUntilFinish();
    }

    public static class HandCoder extends CustomCoder<TestObj> {

        @Override
        public void encode(TestObj value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            VarIntCoder.of().encode(value.getTestInt(), outStream);
            NullableCoder.of(VarIntCoder.of()).encode(value.getTestWrappedInt(), outStream);
            VarLongCoder.of().encode(value.getTestLong(), outStream);
            NullableCoder.of(VarLongCoder.of()).encode(value.getTestWrappedLong(), outStream);
            DoubleCoder.of().encode(value.getTestDouble(), outStream);
            NullableCoder.of(DoubleCoder.of()).encode(value.getTestWrappedDouble(), outStream);
            StringUtf8Coder.of().encode(value.getTestString(), outStream);
            ListCoder.of(StringUtf8Coder.of()).encode(value.getTestSimpleList(), outStream);
            ListCoder.of(NestedCoder.of()).encode(value.getTestComplexList(), outStream);
            MapCoder.of(StringUtf8Coder.of(), VarIntCoder.of()).encode(value.getTestSimpleMap(), outStream);
            MapCoder.of(StringUtf8Coder.of(), NestedCoder.of()).encode(value.getTestComplexMap(), outStream);
        }

        @Override
        public TestObj decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            return new TestObj(
                    VarIntCoder.of().decode(inStream),
                    NullableCoder.of(VarIntCoder.of()).decode(inStream),
            VarLongCoder.of().decode(inStream),
                            NullableCoder.of(VarLongCoder.of()).decode(inStream),
            DoubleCoder.of().decode(inStream),
                                    NullableCoder.of(DoubleCoder.of()).decode(inStream),
            StringUtf8Coder.of().decode(inStream),
            ListCoder.of(StringUtf8Coder.of()).decode(inStream),
            ListCoder.of(NestedCoder.of()).decode(inStream),
            MapCoder.of(StringUtf8Coder.of(), VarIntCoder.of()).decode(inStream),
            MapCoder.of(StringUtf8Coder.of(), NestedCoder.of()).decode(inStream));
        }
    }

    public static class HandCoder2 extends CustomCoder<BasicTestObj> {

        @Override
        public void encode(BasicTestObj value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            VarIntCoder.of().encode(value.getTestInt(), outStream);
            NullableCoder.of(VarIntCoder.of()).encode(value.getTestWrappedInt(), outStream);
            VarLongCoder.of().encode(value.getTestLong(), outStream);
            NullableCoder.of(VarLongCoder.of()).encode(value.getTestWrappedLong(), outStream);
            DoubleCoder.of().encode(value.getTestDouble(), outStream);
            NullableCoder.of(DoubleCoder.of()).encode(value.getTestWrappedDouble(), outStream);
            StringUtf8Coder.of().encode(value.getTestString(), outStream);
        }

        @Override
        public BasicTestObj decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            return new BasicTestObj(
                    VarIntCoder.of().decode(inStream),
                    NullableCoder.of(VarIntCoder.of()).decode(inStream),
                    VarLongCoder.of().decode(inStream),
                    NullableCoder.of(VarLongCoder.of()).decode(inStream),
                    DoubleCoder.of().decode(inStream),
                    NullableCoder.of(DoubleCoder.of()).decode(inStream),
                    StringUtf8Coder.of().decode(inStream));
        }
    }

    @NoArgsConstructor(staticName = "of")
    public static class NestedCoder extends CustomCoder<TestObj.NestedObj> {

        @Override
        public void encode(TestObj.NestedObj value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            VarIntCoder.of().encode(value.getNestedInt(), outStream);
            StringUtf8Coder.of().encode(value.getNestedString(), outStream);
        }

        @Override
        public TestObj.NestedObj decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            return new TestObj.NestedObj(VarIntCoder.of().decode(inStream), StringUtf8Coder.of().decode(inStream));
        }
    }

    public static class ProtoConvert extends DoFn<TestObj, TestObjOuterClass.TestObj> {

        @ProcessElement
        public void processElement(final ProcessContext processContext) {
            final TestObj obj = processContext.element();
            TestObjOuterClass.TestObj.Builder builder = TestObjOuterClass.TestObj.newBuilder();
            builder.setTestInt(obj.getTestInt());
            if (obj.getTestWrappedInt() != null) {
                builder.setTestWrappedInt(Int32Value.of(obj.getTestWrappedInt()));
            }

            builder.setTestLong(obj.getTestLong());
            if (obj.getTestWrappedLong() != null) {
                builder.setTestWrappedLong(Int64Value.of(obj.getTestWrappedLong()));
            }
            builder.setTestDouble(obj.getTestDouble());
            if (obj.getTestWrappedDouble() != null) {
                builder.setTestWrappedDouble(DoubleValue.of(obj.getTestWrappedDouble()));
            }
            builder.setTestString(obj.getTestString())
            .addAllTestSimpleList(obj.getTestSimpleList())
            .addAllTestComplexList(obj.getTestComplexList().stream().map(ProtoConvert::convertNested).collect(Collectors.toList()))
            .putAllTestSimpleMap(obj.getTestSimpleMap())
            .putAllTestComplexMap(obj.getTestComplexMap().entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(), entry -> convertNested(entry.getValue()))));
            processContext.output(builder.build());
        }

        private static TestObjOuterClass.TestObj.NestedObj convertNested(TestObj.NestedObj nestedObj) {
            return TestObjOuterClass.TestObj.NestedObj.newBuilder()
                    .setNestedInt(nestedObj.getNestedInt())
                    .setNestedString(nestedObj.getNestedString())
                    .build();
        }

    }

    public static class ProtoConvert2 extends DoFn<BasicTestObj, BasicTestObjOuterClass.BasicTestObj> {

        @ProcessElement
        public void processElement(final ProcessContext processContext) {
            final BasicTestObj obj = processContext.element();
            BasicTestObjOuterClass.BasicTestObj.Builder builder = BasicTestObjOuterClass.BasicTestObj.newBuilder();
            builder.setTestInt(obj.getTestInt());
            if (obj.getTestWrappedInt() != null) {
                builder.setTestWrappedInt(Int32Value.of(obj.getTestWrappedInt()));
            }

            builder.setTestLong(obj.getTestLong());
            if (obj.getTestWrappedLong() != null) {
                builder.setTestWrappedLong(Int64Value.of(obj.getTestWrappedLong()));
            }
            builder.setTestDouble(obj.getTestDouble());
            if (obj.getTestWrappedDouble() != null) {
                builder.setTestWrappedDouble(DoubleValue.of(obj.getTestWrappedDouble()));
            }
            builder.setTestString(obj.getTestString());
            processContext.output(builder.build());
        }

        private static TestObjOuterClass.TestObj.NestedObj convertNested(TestObj.NestedObj nestedObj) {
            return TestObjOuterClass.TestObj.NestedObj.newBuilder()
                    .setNestedInt(nestedObj.getNestedInt())
                    .setNestedString(nestedObj.getNestedString())
                    .build();
        }

    }

    public static class AvroConvert extends DoFn<TestObj, org.apache.beam.examples.avro.TestObj> {

        @ProcessElement
        public void processElement(final ProcessContext processContext) {
            final TestObj obj = processContext.element();
            org.apache.beam.examples.avro.TestObj.Builder builder = org.apache.beam.examples.avro.TestObj.newBuilder();
            builder.setTestInt(obj.getTestInt());
            builder.setTestWrappedInt(obj.getTestWrappedInt());

            builder.setTestLong(obj.getTestLong());
            builder.setTestWrappedLong(obj.getTestWrappedLong());
            builder.setTestDouble(obj.getTestDouble());
            builder.setTestWrappedDouble(obj.getTestWrappedDouble());
            builder.setTestString(obj.getTestString())
            .setTestSimpleList(obj.getTestSimpleList().stream().map(s -> (CharSequence) s).collect(Collectors.toList()))
            .setTestComplexList(obj.getTestComplexList().stream().map(AvroConvert::convertNested).collect(Collectors.toList()))
            .setTestSimpleMap(obj.getTestSimpleMap().entrySet().stream().collect(Collectors.toMap(entry -> (CharSequence) entry.getKey(), entry -> entry.getValue())))
            .setTestComplexMap(obj.getTestComplexMap().entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(), entry -> convertNested(entry.getValue()))));
            processContext.output(builder.build());
        }

        private static NestedObj convertNested(TestObj.NestedObj nestedObj) {
            return NestedObj.newBuilder()
                    .setNestedInt(nestedObj.getNestedInt())
                    .setNestedString(nestedObj.getNestedString())
                    .build();
        }

    }


    public static class AvroConvert2 extends DoFn<BasicTestObj, org.apache.beam.examples.avro.BasicTestObj> {

        @ProcessElement
        public void processElement(final ProcessContext processContext) {
            final BasicTestObj obj = processContext.element();
            org.apache.beam.examples.avro.BasicTestObj.Builder builder = org.apache.beam.examples.avro.BasicTestObj.newBuilder();
            builder.setTestInt(obj.getTestInt());
            builder.setTestWrappedInt(obj.getTestWrappedInt());

            builder.setTestLong(obj.getTestLong());
            builder.setTestWrappedLong(obj.getTestWrappedLong());
            builder.setTestDouble(obj.getTestDouble());
            builder.setTestWrappedDouble(obj.getTestWrappedDouble());
            builder.setTestString(obj.getTestString());
            processContext.output(builder.build());
        }

        private static NestedObj convertNested(TestObj.NestedObj nestedObj) {
            return NestedObj.newBuilder()
                    .setNestedInt(nestedObj.getNestedInt())
                    .setNestedString(nestedObj.getNestedString())
                    .build();
        }

    }

    public static class ReadFromJson extends DoFn<String, BasicTestObj> {
        private static final ObjectReader OBJECT_MAPPER = new ObjectMapper().registerModule(new BlackbirdModule()).readerFor(BasicTestObj.class);

        @ProcessElement
        public void processElement(final ProcessContext processContext) throws IOException {
            processContext.output(OBJECT_MAPPER.readValue(processContext.element()));
        }
    }
    private static class JsonDelegateCoder {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        public static <T> CustomCoder<T> of(Class<T> clazz) {
            return DelegateCoder.of(StringUtf8Coder.of(), new DelegateCoder.CodingFunction<T, String>() {
                @Override
                public String apply(T input) throws @UnknownKeyFor @NonNull @Initialized Exception {
                    return OBJECT_MAPPER.writeValueAsString(input);
                }
            }, new DelegateCoder.CodingFunction<String, T>() {
                @Override
                public T apply(String input) throws @UnknownKeyFor@NonNull@Initialized Exception {
                    return OBJECT_MAPPER.readValue(input, clazz);
                }
            });
        }
    }

    private static class JsonCoder<T> extends CustomCoder<T> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new BlackbirdModule()).configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        private final Class<T> clazz;

        public JsonCoder(final Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public void encode(T value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            OBJECT_MAPPER.writeValue(outStream, value);
        }

        @Override
        public T decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            return OBJECT_MAPPER.readValue(inStream, clazz);
        }
    }

    public static class FstCoder<T> extends CustomCoder<T> {
        private static final FSTConfiguration CONF = FSTConfiguration.createDefaultConfiguration();
        private final Class<T> clazz;

        private FstCoder(Class<T> clazz) {
            this.clazz = clazz;
            CONF.registerClass(clazz);
        }

        public static <T> FstCoder<T> of(Class<T> clazz) {
            return new FstCoder<T>(clazz);
        }

        @Override
        public void encode(T value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            final FSTObjectOutput out = CONF.getObjectOutput(outStream);
            out.writeObject(value, clazz);
            out.flush();
        }

        @Override
        public T decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            try {
                final FSTObjectInput in = CONF.getObjectInput(inStream);
                return (T) in.readObject(clazz);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
        }
    }



    public static void main(String[] args) throws Exception {
        DataReadingPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DataReadingPipelineOptions.class);

        runWordCount(options);
    }
}

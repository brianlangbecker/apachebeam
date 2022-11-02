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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import javax.annotation.Nullable;

/**
 * To keep {@link MinimalWordCount} simple, it is not factored or testable. This test file should be
 * maintained with a copy of its code for a basic smoke test.
 */
@RunWith(JUnit4.class)
public class MinimalWordCountTest implements Serializable {

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @EqualsAndHashCode
  @ToString
  @DefaultSchema(JavaBeanSchema.class)
  @Getter
  private static class TmpTest {
    private final int testInt;
    private final String testString;
    @Nullable private final Integer testNullInt;
    private final Nested testNested;

    @SchemaCreate
    public TmpTest(int testInt, String testString, Integer testNullInt, Nested testNested) {
      this.testInt = testInt;
      this.testString = testString;
      this.testNullInt = testNullInt;
      this.testNested = testNested;
    }
  }

  @DefaultSchema(JavaBeanSchema.class)
  @Getter
  private static class Nested {
    private final String nestedString;

    @SchemaCreate
    public Nested(final String nestedString) {
      this.nestedString = nestedString;
    }

    public String getNestedString() {
      return nestedString;
    }
  }

  private static class NestedLogicalType implements Schema.LogicalType<Nested, String> {

    @Override
    public @UnknownKeyFor @NonNull @Initialized String getIdentifier() {
      return "My:nested:type";
    }

    @Override
    public Schema.@org.checkerframework.checker.nullness.qual.Nullable @UnknownKeyFor @Initialized FieldType getArgumentType() {
      return Schema.FieldType.STRING;
    }

    @Override
    public Schema.@UnknownKeyFor @NonNull @Initialized FieldType getBaseType() {
      return Schema.FieldType.STRING;
    }

    @Override
    public @NonNull String toBaseType(@NonNull Nested input) {
      return input.getNestedString();
    }

    @Override
    public @NonNull Nested toInputType(@NonNull String base) {
      return new Nested(base);
    }
  }

  @Test
  public void testCoder() throws Exception {
    Schema testSchema = Schema.builder()
            .addInt32Field("testInt")
            .addNullableField("testNullInt", Schema.FieldType.INT32)
            .addStringField("testString")
            .addLogicalTypeField("testNested", new NestedLogicalType())
            .build();
    //p.getSchemaRegistry().registerSchemaForClass(TmpTest.class, testSchema, nes -> Row.withSchema(testSchema).addValues(nes.getTestInt(), nes.getTestNullInt(), nes.getTestString(), nes.getTestNested()).build(), row -> new TmpTest(row.getInt32(0), row.getString(2), row.getInt32(1), row.getValue(3)));
    Coder<TmpTest> coder = p.getSchemaRegistry().getSchemaCoder(TmpTest.class);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      coder.encode(new TmpTest(1, "foo", null, new Nested("bar")), out);
      try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
        coder.decode(in);
      }
    }
  }
  /** A basic smoke test that ensures there is no crash at pipeline construction time. */
  @Test
  public void testMinimalWordCount() throws Exception {
    p.getOptions().as(GcsOptions.class).setGcsUtil(buildMockGcsUtil());

    p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String word) -> Arrays.asList(word.split("[^a-zA-Z']+"))))
        .apply(Filter.by((String word) -> !word.isEmpty()))
        .apply(Count.perElement())
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()))
        .apply(TextIO.write().to("gs://your-output-bucket/and-output-prefix"));
  }

  private GcsUtil buildMockGcsUtil() throws IOException {
    GcsUtil mockGcsUtil = Mockito.mock(GcsUtil.class);

    // Any request to open gets a new bogus channel
    Mockito.when(mockGcsUtil.open(Mockito.any(GcsPath.class)))
        .then(
            invocation ->
                FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.DELETE_ON_CLOSE));

    // Any request for expansion returns a list containing the original GcsPath
    // This is required to pass validation that occurs in TextIO during apply()
    Mockito.when(mockGcsUtil.expand(Mockito.any(GcsPath.class)))
        .then(invocation -> ImmutableList.of((GcsPath) invocation.getArguments()[0]));

    return mockGcsUtil;
  }
}

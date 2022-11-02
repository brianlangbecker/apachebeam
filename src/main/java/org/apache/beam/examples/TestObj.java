package org.apache.beam.examples;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.commons.lang3.RandomStringUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@EqualsAndHashCode
@ToString
@Getter
@DefaultSchema(JavaFieldSchema.class)
public final class TestObj implements Serializable {
    private static final Random RANDOM = new Random();

    private int testInt;
    @Nullable private Integer testWrappedInt;
    private long testLong;
    @Nullable private Long testWrappedLong;
    private double testDouble;
    @Nullable private Double testWrappedDouble;
    @Nullable private String testString;

    private List<String> testSimpleList;
    private List<NestedObj> testComplexList;

    private Map<String, Integer> testSimpleMap;
    private Map<String, NestedObj> testComplexMap;

    @JsonCreator
    @SchemaCreate
    public TestObj(@JsonProperty("testInt") int testInt, @JsonProperty("testWrappedInt") Integer testWrappedInt, @JsonProperty("testLong") long testLong, @JsonProperty("testWrappedLong") Long testWrappedLong, @JsonProperty("testDouble") double testDouble, @JsonProperty("testWrappedDouble") Double testWrappedDouble, @JsonProperty("testString") String testString, @JsonProperty("testSimpleList") List<String> testSimpleList, @JsonProperty("testComplexList") List<NestedObj> testComplexList, @JsonProperty("testSimpleMap") Map<String, Integer> testSimpleMap, @JsonProperty("testComplexMap") Map<String, NestedObj> testComplexMap) {
        this.testInt = testInt;
        this.testWrappedInt = testWrappedInt;
        this.testLong = testLong;
        this.testWrappedLong = testWrappedLong;
        this.testDouble = testDouble;
        this.testWrappedDouble = testWrappedDouble;
        this.testString = testString;
        this.testSimpleList = testSimpleList;
        this.testComplexList = testComplexList;
        this.testSimpleMap = testSimpleMap;
        this.testComplexMap = testComplexMap;
    }

    public TestObj() {
    }

    @Data
    @DefaultSchema(JavaFieldSchema.class)
    public static final class NestedObj implements Serializable {
        private int nestedInt;
        private String nestedString;

        @JsonCreator
        @SchemaCreate
        public NestedObj(@JsonProperty("nestedInt") int nestedInt, @JsonProperty("nestedString") String nestedString) {
            this.nestedInt = nestedInt;
            this.nestedString = nestedString;
        }

        public NestedObj() {
        }

        static NestedObj randomInstance() {
            return new NestedObj(RANDOM.nextInt(), RandomStringUtils.random(RANDOM.nextInt(10)));
        }
    }

    public static TestObj randomInstance() {
        return new TestObj(RANDOM.nextInt(),
                RANDOM.nextBoolean() ? null : RANDOM.nextInt(),
                RANDOM.nextLong(),
                RANDOM.nextBoolean() ? null : RANDOM.nextLong(),
                RANDOM.nextDouble(),
                RANDOM.nextBoolean() ? null : RANDOM.nextDouble(),
                RandomStringUtils.random(RANDOM.nextInt(10)),
                IntStream.range(0, RANDOM.nextInt(10)).mapToObj(unused -> RandomStringUtils.random(RANDOM.nextInt(100))).collect(Collectors.toList()),
                IntStream.range(0, RANDOM.nextInt(10)).mapToObj(unused -> NestedObj.randomInstance()).collect(Collectors.toList()),
                IntStream.range(0, RANDOM.nextInt(10)).mapToObj(unused -> RandomStringUtils.random(10)).collect(Collectors.toMap(Function.identity(), unused -> RANDOM.nextInt(10))),
                IntStream.range(0, RANDOM.nextInt(10)).mapToObj(unused -> RandomStringUtils.random(10)).collect(Collectors.toMap(Function.identity(),  unused -> NestedObj.randomInstance()))
                );
    }
}

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
@DefaultSchema(JavaBeanSchema.class)
public final class BasicTestObj implements Serializable {
    private static final Random RANDOM = new Random();

    private int testInt;
    @Nullable private Integer testWrappedInt;
    private long testLong;
    @Nullable private Long testWrappedLong;
    private double testDouble;
    @Nullable private Double testWrappedDouble;
    @Nullable private String testString;

    @JsonCreator
    @SchemaCreate
    public BasicTestObj(@JsonProperty("testInt") int testInt, @JsonProperty("testWrappedInt") Integer testWrappedInt, @JsonProperty("testLong") long testLong, @JsonProperty("testWrappedLong") Long testWrappedLong, @JsonProperty("testDouble") double testDouble, @JsonProperty("testWrappedDouble") Double testWrappedDouble, @JsonProperty("testString") String testString) {
        this.testInt = testInt;
        this.testWrappedInt = testWrappedInt;
        this.testLong = testLong;
        this.testWrappedLong = testWrappedLong;
        this.testDouble = testDouble;
        this.testWrappedDouble = testWrappedDouble;
        this.testString = testString;
    }

    public BasicTestObj() {
    }

    public static BasicTestObj randomInstance() {
        return new BasicTestObj(RANDOM.nextInt(),
                RANDOM.nextBoolean() ? null : Math.abs(RANDOM.nextInt(100)),
                RANDOM.nextLong(),
                RANDOM.nextBoolean() ? null : (long) Math.abs(RANDOM.nextInt(100)),
                RANDOM.nextDouble(),
                RANDOM.nextBoolean() ? null : RANDOM.nextDouble(),
                RandomStringUtils.random(RANDOM.nextInt(10)));
    }
}

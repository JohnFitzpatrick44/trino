/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.variant;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.block.VariantBlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.VariantShreddingSchema.ArraySchema;
import io.trino.spi.variant.VariantShreddingSchema.Field;
import io.trino.spi.variant.VariantShreddingSchema.ObjectSchema;
import io.trino.spi.variant.VariantShreddingSchema.Primitive;
import io.trino.spi.variant.VariantShreddingSchema.ValueGroup;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestVariantShredding
{
    private static final List<Type> PRIMITIVE_TYPES = List.of(
            BOOLEAN,
            TINYINT,
            SMALLINT,
            INTEGER,
            BIGINT,
            REAL,
            DOUBLE,
            DecimalType.createDecimalType(9, 2),
            DecimalType.createDecimalType(18, 4),
            DecimalType.createDecimalType(38, 10),
            DATE,
            TIME_MICROS,
            TIMESTAMP_MICROS,
            TIMESTAMP_NANOS,
            TIMESTAMP_TZ_MICROS,
            TIMESTAMP_TZ_NANOS,
            VARBINARY,
            VARCHAR,
            UUID);
    private static final List<String> FIELD_NAMES = List.of("a", "b", "c", "event_type", "event_ts", "Mixed Case", "user.name", "");

    private static final ObjectSchema EVENT_SCHEMA = new ObjectSchema(List.of(
            new Field(utf8Slice("event_ts"), ValueGroup.typed(new Primitive(TIMESTAMP_TZ_MICROS))),
            new Field(utf8Slice("event_type"), ValueGroup.typed(new Primitive(VARCHAR)))));

    @Test
    void testSpecObjectExample()
    {
        // the event examples from the shredding specification
        List<Variant> values = Arrays.asList(
                object("event_type", Variant.ofString("noop"), "event_ts", Variant.ofTimestampMicrosUtc(1729794114937L)),
                object("event_type", Variant.ofString("login"), "event_ts", Variant.ofTimestampMicrosUtc(1729794146402L), "email", Variant.ofString("user@example.com")),
                object("error_msg", Variant.ofString("malformed: ...")),
                Variant.ofString("malformed: not an object"),
                object("event_ts", Variant.ofTimestampMicrosUtc(1729794240241L), "click", Variant.ofString("_button")),
                object("event_type", Variant.NULL_VALUE, "event_ts", Variant.ofTimestampMicrosUtc(1729794954163L)),
                object("event_type", Variant.ofString("noop"), "event_ts", Variant.ofString("2024-10-24")),
                Variant.EMPTY_OBJECT,
                Variant.NULL_VALUE,
                null);

        VariantBlock block = VariantShredder.shred(createBlock(values), EVENT_SCHEMA);
        assertThat(block.isShredded()).isTrue();
        assertRoundTrip(block, values);

        Block residual = block.getResidualValues();
        RowBlock typed = (RowBlock) block.getTypedValue();
        RowBlock eventTs = (RowBlock) typed.getFieldBlock(0);
        RowBlock eventType = (RowBlock) typed.getFieldBlock(1);

        // fully shredded object
        assertThat(residual.isNull(0)).isTrue();
        assertThat(typed.isNull(0)).isFalse();
        assertThat(VARCHAR.getSlice(eventType.getFieldBlock(1), 0).toStringUtf8()).isEqualTo("noop");
        // partially shredded object
        assertThat(Variant.from(Metadata.EMPTY_METADATA, VARBINARY.getSlice(residual, 1)).basicType()).isEqualTo(Header.BasicType.OBJECT);
        // object with all shredded fields missing
        assertThat(residual.isNull(2)).isFalse();
        assertThat(eventType.getFieldBlock(0).isNull(2)).isTrue();
        assertThat(eventType.getFieldBlock(1).isNull(2)).isTrue();
        // not an object
        assertThat(typed.isNull(3)).isTrue();
        assertThat(residual.isNull(3)).isFalse();
        // field present with variant null is stored in the field value
        assertThat(VARBINARY.getSlice(eventType.getFieldBlock(0), 5)).isEqualTo(Variant.NULL_VALUE.data());
        // field with a different type is stored in the field value
        assertThat(eventTs.getFieldBlock(1).isNull(6)).isTrue();
        assertThat(eventTs.getFieldBlock(0).isNull(6)).isFalse();
        // empty object
        assertThat(typed.isNull(7)).isFalse();
        assertThat(residual.isNull(7)).isTrue();
        // variant null
        assertThat(typed.isNull(8)).isTrue();
        assertThat(VARBINARY.getSlice(residual, 8)).isEqualTo(Variant.NULL_VALUE.data());
        // null
        assertThat(block.isNull(9)).isTrue();
    }

    @Test
    void testSpecArrayExample()
    {
        ArraySchema schema = new ArraySchema(ValueGroup.typed(new Primitive(VARCHAR)));
        List<Variant> values = Arrays.asList(
                Variant.ofArray(List.of(Variant.ofString("comedy"), Variant.ofString("drama"))),
                Variant.ofArray(List.of(Variant.ofString("horror"), Variant.NULL_VALUE)),
                Variant.ofArray(List.of(Variant.ofString("comedy"), Variant.ofString("drama"), Variant.ofString("romance"))),
                Variant.NULL_VALUE,
                Variant.EMPTY_ARRAY,
                Variant.ofLong(1));
        VariantBlock block = VariantShredder.shred(createBlock(values), schema);
        assertRoundTrip(block, values);
        assertThat(block.getResidualValues().isNull(0)).isTrue();
        assertThat(block.getTypedValue().isNull(3)).isTrue();
        assertThat(block.getTypedValue().isNull(5)).isTrue();
    }

    @Test
    void testPrimitiveShredding()
    {
        for (Type type : PRIMITIVE_TYPES) {
            Random random = new Random(type.hashCode());
            List<Variant> values = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                values.add(random.nextInt(10) == 0 ? null : randomPrimitive(random));
            }
            assertRoundTrip(VariantShredder.shred(createBlock(values), new Primitive(type)), values);
        }
    }

    @Test
    void testMissingTopLevelValueIsVariantNull()
    {
        Block metadata = varbinary(Metadata.EMPTY_METADATA_SLICE);
        Block residual = varbinary((Slice) null);
        BlockBuilder typed = BIGINT.createBlockBuilder(null, 1);
        typed.appendNull();
        VariantBlock block = VariantBlock.createShredded(1, metadata, residual, new Primitive(BIGINT), typed.build(), Optional.empty());
        assertThat(block.getVariant(0)).isEqualTo(Variant.NULL_VALUE);
    }

    @Test
    void testFieldWithValueAndTypedValueFails()
    {
        VariantBlock block = VariantShredder.shred(createBlock(List.of(object("a", Variant.ofLong(1)))), new ObjectSchema(List.of(new Field(utf8Slice("a"), ValueGroup.typed(new Primitive(BIGINT))))));
        RowBlock typed = (RowBlock) block.getTypedValue();
        RowBlock field = (RowBlock) typed.getFieldBlock(0);
        // corrupt the field so both value and typed_value are present
        RowBlock corruptField = RowBlock.fromFieldBlocks(1, new Block[] {varbinary(Variant.ofLong(2).data()), field.getFieldBlock(1)});
        VariantBlock corrupt = VariantBlock.createShredded(
                1,
                block.getMetadata(),
                block.getResidualValues(),
                block.getShreddingSchema().orElseThrow(),
                RowBlock.fromFieldBlocks(1, new Block[] {corruptField}),
                Optional.empty());
        assertThatThrownBy(() -> corrupt.getVariant(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field a has both a value and a typed_value");
    }

    @Test
    void testPartiallyShreddedValueMustBeObject()
    {
        ObjectSchema schema = new ObjectSchema(List.of(new Field(utf8Slice("a"), ValueGroup.typed(new Primitive(BIGINT)))));
        VariantBlock block = VariantShredder.shred(createBlock(List.of(object("a", Variant.ofLong(1)))), schema);
        VariantBlock corrupt = VariantBlock.createShredded(
                1,
                block.getMetadata(),
                varbinary(Variant.ofString("a").data()),
                schema,
                block.getTypedValue(),
                Optional.empty());
        assertThatThrownBy(() -> corrupt.getVariant(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partially shredded value must be an object");
    }

    @Test
    void testRandomRoundTrip()
    {
        Random random = new Random(42);
        for (int iteration = 0; iteration < 200; iteration++) {
            VariantShreddingSchema schema = randomSchema(random, 0);
            List<Variant> values = new ArrayList<>();
            int positionCount = random.nextInt(50);
            for (int i = 0; i < positionCount; i++) {
                values.add(random.nextInt(10) == 0 ? null : randomValue(random, 0));
            }
            VariantBlock block = VariantShredder.shred(createBlock(values), schema);
            assertRoundTrip(block, values);
            assertBlockOperations(block, values);
            // shredding an equivalent shredded block with a different schema reshreds the values
            assertRoundTrip(VariantShredder.shred(block, randomSchema(random, 0)), values);
        }
    }

    @Test
    void testEncodingRoundTrip()
    {
        BlockEncodingSerde serde = new TestingBlockEncodingSerde();
        ObjectSchema schema = new ObjectSchema(List.of(
                new Field(utf8Slice("a"), ValueGroup.typed(new Primitive(BIGINT))),
                new Field(utf8Slice("b"), ValueGroup.typed(new ArraySchema(ValueGroup.typed(new Primitive(VARCHAR))))),
                new Field(utf8Slice("c"), ValueGroup.valueOnly()),
                new Field(utf8Slice("d"), ValueGroup.typed(new ObjectSchema(List.of(new Field(utf8Slice("x"), ValueGroup.typed(new Primitive(DOUBLE)))))))));
        Random random = new Random(7);
        List<Variant> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add(random.nextInt(10) == 0 ? null : randomValue(random, 0));
        }
        VariantBlock block = VariantShredder.shred(createBlock(values), schema);
        for (Block input : List.of(block, block.getRegion(10, 50))) {
            DynamicSliceOutput output = new DynamicSliceOutput(1024);
            serde.writeBlock(output, input);
            VariantBlock actual = (VariantBlock) serde.readBlock(output.slice().getInput());
            assertThat(actual.getShreddingSchema()).contains(schema);
            assertSameValues(actual, input);
        }
    }

    @Test
    void testBuilderAppendsShreddedValues()
    {
        Random random = new Random(3);
        List<Variant> values = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            values.add(random.nextInt(8) == 0 ? null : randomValue(random, 0));
        }
        VariantBlock block = VariantShredder.shred(createBlock(values), randomSchema(random, 0));

        VariantBlockBuilder builder = VARIANT.createBlockBuilder(null, 10);
        builder.append(block, 3);
        builder.appendRange(block, 5, 10);
        builder.appendRepeated(block, 7, 3);
        builder.appendPositions(block, new int[] {1, 9, 2, 40}, 1, 3);
        List<Variant> expected = new ArrayList<>();
        expected.add(values.get(3));
        expected.addAll(values.subList(5, 15));
        expected.addAll(Collections.nCopies(3, values.get(7)));
        expected.add(values.get(9));
        expected.add(values.get(2));
        expected.add(values.get(40));
        VariantBlock actual = builder.buildValueBlock();
        assertThat(actual.isShredded()).isFalse();
        assertRoundTrip(actual, expected);
    }

    private static void assertBlockOperations(VariantBlock block, List<Variant> values)
    {
        if (values.isEmpty()) {
            return;
        }
        int offset = values.size() / 3;
        int length = values.size() - offset;
        assertRoundTrip(block.getRegion(offset, length), values.subList(offset, offset + length));
        assertRoundTrip(block.copyRegion(offset, length), values.subList(offset, offset + length));
        assertRoundTrip(block.getRegion(offset, length).unshred(), values.subList(offset, offset + length));

        int[] positions = {values.size() - 1, 0, values.size() / 2};
        assertRoundTrip(block.copyPositions(positions, 0, positions.length), Arrays.stream(positions).mapToObj(values::get).toList());
        assertRoundTrip(block.getSingleValueBlock(values.size() - 1), Arrays.asList(values.get(values.size() - 1)));

        List<Variant> withNull = new ArrayList<>(values);
        withNull.add(null);
        assertRoundTrip(block.copyWithAppendedNull(), withNull);

        assertThat(block.getSizeInBytes()).isPositive();
        assertThat(block.getRetainedSizeInBytes()).isGreaterThanOrEqualTo(block.getSizeInBytes());
        for (int position = 0; position < values.size(); position++) {
            if (values.get(position) != null) {
                assertThat(block.getBasicType(position)).isEqualTo(values.get(position).basicType());
            }
        }
    }

    private static void assertSameValues(VariantBlock actual, Block expected)
    {
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertThat(actual.isNull(position)).isEqualTo(expected.isNull(position));
            if (!actual.isNull(position)) {
                assertThat(actual.getVariant(position).toObject()).isEqualTo(VARIANT.getObject(expected, position).toObject());
            }
        }
    }

    private static void assertRoundTrip(VariantBlock block, List<Variant> expected)
    {
        assertThat(block.getPositionCount()).isEqualTo(expected.size());
        VariantBlock unshredded = block.unshred();
        for (int position = 0; position < expected.size(); position++) {
            Variant expectedValue = expected.get(position);
            assertThat(block.isNull(position)).isEqualTo(expectedValue == null);
            assertThat(unshredded.isNull(position)).isEqualTo(expectedValue == null);
            if (expectedValue != null) {
                Variant actual = block.getVariant(position);
                assertThat(actual).isEqualTo(expectedValue);
                // the physical types are preserved, not only the logical value
                assertThat(actual.toObject()).isEqualTo(expectedValue.toObject());
                assertThat(unshredded.getVariant(position).toObject()).isEqualTo(expectedValue.toObject());
            }
        }
    }

    private static VariantBlock createBlock(List<Variant> values)
    {
        VariantBlockBuilder builder = VARIANT.createBlockBuilder(null, values.size());
        for (Variant value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(value);
            }
        }
        return builder.buildValueBlock();
    }

    private static Block varbinary(Slice... values)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, values.length, 16);
        for (Slice value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(value);
            }
        }
        return builder.build();
    }

    private static Variant object(Object... fieldsAndValues)
    {
        Map<Slice, Variant> fields = new LinkedHashMap<>();
        for (int i = 0; i < fieldsAndValues.length; i += 2) {
            fields.put(utf8Slice((String) fieldsAndValues[i]), (Variant) fieldsAndValues[i + 1]);
        }
        return Variant.ofObject(fields);
    }

    private static VariantShreddingSchema randomSchema(Random random, int depth)
    {
        int kind = depth >= 3 ? 0 : random.nextInt(3);
        return switch (kind) {
            case 0 -> new Primitive(PRIMITIVE_TYPES.get(random.nextInt(PRIMITIVE_TYPES.size())));
            case 1 -> {
                List<Field> fields = new ArrayList<>();
                FIELD_NAMES.stream()
                        .map(Slices::utf8Slice)
                        .sorted()
                        .filter(_ -> random.nextBoolean())
                        .forEach(name -> fields.add(new Field(name, randomGroup(random, depth + 1))));
                yield new ObjectSchema(fields);
            }
            default -> new ArraySchema(randomGroup(random, depth + 1));
        };
    }

    private static ValueGroup randomGroup(Random random, int depth)
    {
        if (random.nextInt(4) == 0) {
            return ValueGroup.valueOnly();
        }
        return ValueGroup.typed(randomSchema(random, depth));
    }

    private static Variant randomValue(Random random, int depth)
    {
        int kind = depth >= 3 ? 0 : random.nextInt(4);
        return switch (kind) {
            case 0, 1 -> randomPrimitive(random);
            case 2 -> {
                Map<Slice, Variant> fields = new LinkedHashMap<>();
                for (String name : FIELD_NAMES) {
                    if (random.nextBoolean()) {
                        fields.put(utf8Slice(name), randomValue(random, depth + 1));
                    }
                }
                // occasionally include fields that are never shredded
                if (random.nextBoolean()) {
                    fields.put(utf8Slice("extra" + random.nextInt(3)), randomValue(random, depth + 1));
                }
                yield Variant.ofObject(fields);
            }
            default -> {
                List<Variant> elements = new ArrayList<>();
                int length = random.nextInt(4);
                for (int i = 0; i < length; i++) {
                    elements.add(randomValue(random, depth + 1));
                }
                yield Variant.ofArray(elements);
            }
        };
    }

    private static Variant randomPrimitive(Random random)
    {
        return switch (random.nextInt(21)) {
            case 0 -> Variant.NULL_VALUE;
            case 1 -> Variant.ofBoolean(random.nextBoolean());
            case 2 -> Variant.ofByte((byte) random.nextInt());
            case 3 -> Variant.ofShort((short) random.nextInt());
            case 4 -> Variant.ofInt(random.nextInt());
            case 5 -> Variant.ofLong(random.nextLong());
            case 6 -> Variant.ofFloat(random.nextFloat());
            case 7 -> Variant.ofDouble(random.nextDouble());
            case 8 -> Variant.ofDecimal(BigDecimal.valueOf(random.nextInt(1_000_000), 2));
            case 9 -> Variant.ofDecimal(BigDecimal.valueOf(random.nextLong() / 1000, 4));
            case 10 -> Variant.ofDecimal(new BigDecimal(new BigInteger(100, random), 10));
            case 11 -> Variant.ofDate(random.nextInt(100_000));
            case 12 -> Variant.ofTimeMicrosNtz(random.nextLong(86_400_000_000L));
            case 13 -> Variant.ofTimestampMicrosNtz(random.nextLong(-1L << 50, 1L << 50));
            case 14 -> Variant.ofTimestampNanosNtz(random.nextLong(-1L << 60, 1L << 60));
            case 15 -> Variant.ofTimestampMicrosUtc(random.nextLong(-1L << 50, 1L << 50));
            case 16 -> Variant.ofTimestampNanosUtc(random.nextLong(-1L << 60, 1L << 60));
            case 17 -> {
                byte[] bytes = new byte[random.nextInt(10)];
                random.nextBytes(bytes);
                yield Variant.ofBinary(wrappedBuffer(bytes));
            }
            case 18 -> Variant.ofString("short" + random.nextInt(100));
            case 19 -> Variant.ofString("a long string that does not fit in a short string encoding at all " + random.nextInt());
            default -> Variant.ofUuid(new UUID(random.nextLong(), random.nextLong()));
        };
    }
}

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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.VariantFields;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.block.VariantBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import io.trino.spi.variant.VariantShredder;
import io.trino.spi.variant.VariantShreddingSchema;
import io.trino.spi.variant.VariantShreddingSchema.ArraySchema;
import io.trino.spi.variant.VariantShreddingSchema.Field;
import io.trino.spi.variant.VariantShreddingSchema.ObjectSchema;
import io.trino.spi.variant.VariantShreddingSchema.Primitive;
import io.trino.spi.variant.VariantShreddingSchema.ValueGroup;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
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
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.util.stream.Collectors.toCollection;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;

final class TestShreddedVariants
{
    private static final List<Type> PRIMITIVE_TYPES = ImmutableList.of(
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
            io.trino.spi.type.UuidType.UUID);
    private static final List<String> FIELD_NAMES = ImmutableList.of("a", "b", "Event_Type", "event_type", "user.name");

    @TempDir
    private Path directory;

    @Test
    void testSpecObjectExample()
            throws IOException
    {
        ObjectSchema schema = new ObjectSchema(ImmutableList.of(
                new Field(utf8Slice("event_ts"), ValueGroup.typed(new Primitive(TIMESTAMP_TZ_MICROS))),
                new Field(utf8Slice("event_type"), ValueGroup.typed(new Primitive(VARCHAR))),
                new Field(utf8Slice("location"), ValueGroup.typed(new ObjectSchema(ImmutableList.of(
                        new Field(utf8Slice("latitude"), ValueGroup.typed(new Primitive(DOUBLE))),
                        new Field(utf8Slice("longitude"), ValueGroup.typed(new Primitive(DOUBLE))))))),
                new Field(utf8Slice("tags"), ValueGroup.typed(new ArraySchema(ValueGroup.typed(new Primitive(VARCHAR)))))));
        List<Variant> values = Arrays.asList(
                object("event_type", Variant.ofString("noop"), "event_ts", Variant.ofTimestampMicrosUtc(1729794114937L)),
                object("event_type", Variant.ofString("login"), "event_ts", Variant.ofTimestampMicrosUtc(1729794146402L), "email", Variant.ofString("user@example.com")),
                object("error_msg", Variant.ofString("malformed: ...")),
                Variant.ofString("malformed: not an object"),
                object("event_ts", Variant.ofTimestampMicrosUtc(1729794240241L), "click", Variant.ofString("_button")),
                object("event_type", Variant.NULL_VALUE, "event_ts", Variant.ofTimestampMicrosUtc(1729794954163L)),
                object("event_type", Variant.ofString("noop"), "event_ts", Variant.ofString("2024-10-24")),
                object("location", object("latitude", Variant.ofDouble(1.5), "longitude", Variant.ofDouble(-2.5), "altitude", Variant.ofLong(3)), "tags", Variant.ofArray(ImmutableList.of(Variant.ofString("a"), Variant.NULL_VALUE))),
                object("location", Variant.ofString("unknown"), "tags", Variant.EMPTY_ARRAY),
                Variant.EMPTY_OBJECT,
                Variant.NULL_VALUE,
                null);
        assertRoundTrip(schema, values);
    }

    @Test
    void testUnannotatedVariantGroup()
            throws IOException
    {
        // field names inside the variant are case-sensitive, even when the writer does not annotate the group
        ObjectSchema schema = new ObjectSchema(ImmutableList.of(
                new Field(utf8Slice("Event_Type"), ValueGroup.typed(new Primitive(VARCHAR))),
                new Field(utf8Slice("event_type"), ValueGroup.typed(new Primitive(BIGINT)))));
        List<Variant> values = Arrays.asList(
                object("Event_Type", Variant.ofString("upper"), "event_type", Variant.ofLong(1)),
                object("event_type", Variant.ofString("not a bigint")),
                null);
        VariantBlock shredded = VariantShredder.shred(createBlock(values), schema);
        Path file = directory.resolve("unannotated.parquet");
        GroupType annotated = VariantFields.toParquetType("v", OPTIONAL, Optional.of(schema));
        writeFile(file, shredded, new GroupType(OPTIONAL, "v", annotated.getFields()));
        assertValues(readVariants(file), values);
    }

    @Test
    void testPrimitives()
            throws IOException
    {
        for (Type type : PRIMITIVE_TYPES) {
            Random random = new Random(type.hashCode());
            List<Variant> values = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                values.add(random.nextInt(10) == 0 ? null : randomPrimitive(random));
            }
            assertRoundTrip(new Primitive(type), values);
        }
    }

    @Test
    void testRandomSchemas()
            throws IOException
    {
        Random random = new Random(11);
        for (int iteration = 0; iteration < 40; iteration++) {
            VariantShreddingSchema schema = randomSchema(random, 0);
            List<Variant> values = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                values.add(random.nextInt(10) == 0 ? null : randomValue(random, 0));
            }
            assertRoundTrip(schema, values);
        }
    }

    @Test
    void testMissingValueColumn()
            throws IOException
    {
        // the value field may be omitted when every value matches the shredded type
        MessageType messageType = new MessageType("test", new GroupType(
                OPTIONAL,
                "v",
                org.apache.parquet.schema.Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("metadata"),
                org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("typed_value")));
        Path file = directory.resolve("missing-value.parquet");
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(messageType).withWriteMode(OVERWRITE).build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(messageType);
            for (long value = 0; value < 10; value++) {
                Group row = factory.newGroup();
                Group variant = row.addGroup("v");
                variant.append("metadata", binary(io.trino.spi.variant.Metadata.EMPTY_METADATA_SLICE));
                variant.append("typed_value", value);
                writer.write(row);
            }
        }
        List<Variant> expected = new ArrayList<>();
        for (long value = 0; value < 10; value++) {
            expected.add(Variant.ofLong(value));
        }
        assertValues(readVariants(file), expected);
    }

    @Test
    void testTrinoWriter()
            throws IOException
    {
        Random random = new Random(5);
        for (int iteration = 0; iteration < 20; iteration++) {
            VariantShreddingSchema schema = randomSchema(random, 0);
            List<Variant> values = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                values.add(random.nextInt(10) == 0 ? null : randomValue(random, 0));
            }
            VariantBlock unshredded = createBlock(values);
            MessageType messageType = new MessageType("test", VariantFields.toParquetType("v", OPTIONAL, Optional.of(schema)));

            // shred while writing
            assertValues(writeAndRead(messageType, unshredded), values);
            // input already shredded with the same schema is written as is
            assertValues(writeAndRead(messageType, VariantShredder.shred(unshredded, schema)), values);
            // input shredded with a different schema is reshredded
            assertValues(writeAndRead(messageType, VariantShredder.shred(unshredded, randomSchema(random, 0))), values);
            // dictionary encoded input
            assertValues(writeAndRead(messageType, DictionaryBlock.create(values.size(), unshredded, IntStream.range(0, values.size()).toArray())), values);
        }
    }

    @Test
    void testTrinoWriterStatistics()
            throws IOException
    {
        ObjectSchema schema = new ObjectSchema(ImmutableList.of(new Field(utf8Slice("id"), ValueGroup.typed(new Primitive(BIGINT)))));
        List<Variant> values = new ArrayList<>();
        for (long id = 10; id < 100; id++) {
            values.add(object("id", Variant.ofLong(id), "other", Variant.ofString("x" + id)));
        }
        MessageType messageType = new MessageType("test", VariantFields.toParquetType("v", OPTIONAL, Optional.of(schema)));
        Path file = directory.resolve("statistics.parquet");
        Files.write(file, writeParquetFile(ParquetWriterOptions.builder().build(), messageType, ImmutableMap.of(), ImmutableList.of(new Page(createBlock(values)))).getBytes());

        ParquetMetadata parquetMetadata;
        try (ParquetDataSource dataSource = new FileParquetDataSource(file.toFile(), ParquetReaderOptions.defaultOptions())) {
            parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        }
        ColumnChunkMetadata idColumn = parquetMetadata.getBlocks().getFirst().columns().stream()
                .filter(column -> column.getPath().toDotString().equals("v.typed_value.id.typed_value"))
                .collect(onlyElement());
        assertThat(idColumn.getStatistics().genericGetMin()).isEqualTo(10L);
        assertThat(idColumn.getStatistics().genericGetMax()).isEqualTo(99L);
        ColumnChunkMetadata idValueColumn = parquetMetadata.getBlocks().getFirst().columns().stream()
                .filter(column -> column.getPath().toDotString().equals("v.typed_value.id.value"))
                .collect(onlyElement());
        assertThat(idValueColumn.getStatistics().getNumNulls()).isEqualTo(values.size());
        assertValues(readVariants(file), values);
    }

    private List<Block> writeAndRead(MessageType messageType, Block block)
            throws IOException
    {
        Path file = directory.resolve("trino-written.parquet");
        Files.write(file, writeParquetFile(ParquetWriterOptions.builder().setMaxPageValueCount(37).build(), messageType, ImmutableMap.of(), ImmutableList.of(new Page(block))).getBytes());
        return readVariants(file);
    }

    private void assertRoundTrip(VariantShreddingSchema schema, List<Variant> values)
            throws IOException
    {
        VariantBlock shredded = VariantShredder.shred(createBlock(values), schema);
        Path file = directory.resolve("shredded.parquet");
        writeFile(file, shredded);
        List<Block> blocks = readVariants(file);
        assertThat(blocks).allSatisfy(block -> assertThat(((VariantBlock) block.getUnderlyingValueBlock()).isShredded()).isTrue());
        assertValues(blocks, values);
    }

    private static void assertValues(List<Block> blocks, List<Variant> expected)
    {
        List<Variant> actual = new ArrayList<>();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                actual.add(block.isNull(position) ? null : VARIANT.getObject(block, position));
            }
        }
        assertThat(actual).hasSize(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            if (expected.get(i) == null) {
                assertThat(actual.get(i)).isNull();
            }
            else {
                assertThat(actual.get(i).toObject()).isEqualTo(expected.get(i).toObject());
            }
        }
    }

    private static List<Block> readVariants(Path file)
            throws IOException
    {
        ParquetDataSource dataSource = new FileParquetDataSource(file.toFile(), ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        ImmutableList.Builder<Block> blocks = ImmutableList.builder();
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, ImmutableList.of(VARIANT), ImmutableList.of("v"))) {
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                blocks.add(page.getBlock(0));
            }
        }
        return blocks.build();
    }

    private static void writeFile(Path file, VariantBlock block)
            throws IOException
    {
        writeFile(file, block, VariantFields.toParquetType("v", OPTIONAL, block.getShreddingSchema()));
    }

    private static void writeFile(Path file, VariantBlock block, GroupType variantType)
            throws IOException
    {
        VariantShreddingSchema schema = block.getShreddingSchema().orElseThrow();
        MessageType messageType = new MessageType("test", variantType);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(messageType)
                .withWriteMode(OVERWRITE)
                .withRowGroupRowCountLimit(97)
                .withPageRowCountLimit(13)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(messageType);
            Block metadata = block.getMetadata();
            Block residual = block.getResidualValues();
            Block typed = block.getTypedValue();
            for (int position = 0; position < block.getPositionCount(); position++) {
                Group row = factory.newGroup();
                if (!block.isNull(position)) {
                    Group variant = row.addGroup("v");
                    variant.append("metadata", binary(VARBINARY.getSlice(metadata, position)));
                    if (!residual.isNull(position)) {
                        variant.append("value", binary(VARBINARY.getSlice(residual, position)));
                    }
                    if (!typed.isNull(position)) {
                        writeTyped(variant, schema, typed, position);
                    }
                }
                writer.write(row);
            }
        }
    }

    private static void writeTyped(Group parent, VariantShreddingSchema schema, Block block, int position)
    {
        switch (schema) {
            case Primitive primitive -> writePrimitive(parent, primitive.type(), block, position);
            case ObjectSchema objectSchema -> {
                Group object = parent.addGroup("typed_value");
                RowBlock rowBlock = (RowBlock) block;
                for (int field = 0; field < objectSchema.fields().size(); field++) {
                    Group fieldGroup = object.addGroup(objectSchema.fields().get(field).name().toStringUtf8());
                    writeValueGroup(fieldGroup, objectSchema.fields().get(field).group(), rowBlock.getFieldBlock(field), position);
                }
            }
            case ArraySchema arraySchema -> {
                Group list = parent.addGroup("typed_value");
                ArrayBlock arrayBlock = (ArrayBlock) block;
                int start = arrayBlock.getRawOffsets()[arrayBlock.getOffsetBase() + position];
                int end = arrayBlock.getRawOffsets()[arrayBlock.getOffsetBase() + position + 1];
                for (int element = start; element < end; element++) {
                    Group elementGroup = list.addGroup("list").addGroup("element");
                    writeValueGroup(elementGroup, arraySchema.element(), arrayBlock.getRawElementBlock(), element);
                }
            }
        }
    }

    private static void writeValueGroup(Group group, ValueGroup schema, Block block, int position)
    {
        RowBlock rowBlock = (RowBlock) block;
        Block value = rowBlock.getFieldBlock(0);
        if (!value.isNull(position)) {
            group.append("value", binary(VARBINARY.getSlice(value, position)));
        }
        if (schema.typedValue().isPresent() && !rowBlock.getFieldBlock(1).isNull(position)) {
            writeTyped(group, schema.typedValue().get(), rowBlock.getFieldBlock(1), position);
        }
    }

    private static void writePrimitive(Group group, Type type, Block block, int position)
    {
        String name = "typed_value";
        if (type.equals(BOOLEAN)) {
            group.append(name, BOOLEAN.getBoolean(block, position));
        }
        else if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(DATE)) {
            group.append(name, (int) type.getLong(block, position));
        }
        else if (type.equals(BIGINT) || type.equals(TIMESTAMP_MICROS)) {
            group.append(name, type.getLong(block, position));
        }
        else if (type.equals(REAL)) {
            group.append(name, REAL.getFloat(block, position));
        }
        else if (type.equals(DOUBLE)) {
            group.append(name, DOUBLE.getDouble(block, position));
        }
        else if (type instanceof DecimalType decimalType) {
            if (decimalType.getPrecision() <= 9) {
                group.append(name, (int) type.getLong(block, position));
            }
            else if (decimalType.getPrecision() <= 18) {
                group.append(name, type.getLong(block, position));
            }
            else {
                BigInteger unscaled = ((Int128) type.getObject(block, position)).toBigInteger();
                group.append(name, Binary.fromConstantByteArray(toFixedBytes(unscaled)));
            }
        }
        else if (type.equals(TIME_MICROS)) {
            group.append(name, type.getLong(block, position) / 1_000_000);
        }
        else if (type.equals(TIMESTAMP_NANOS)) {
            LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
            group.append(name, timestamp.getEpochMicros() * 1000 + timestamp.getPicosOfMicro() / 1000);
        }
        else if (type.equals(TIMESTAMP_TZ_MICROS)) {
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);
            group.append(name, timestamp.getEpochMillis() * 1000 + timestamp.getPicosOfMilli() / 1_000_000);
        }
        else if (type.equals(TIMESTAMP_TZ_NANOS)) {
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);
            group.append(name, timestamp.getEpochMillis() * 1_000_000 + timestamp.getPicosOfMilli() / 1000);
        }
        else {
            // VARBINARY, VARCHAR and UUID
            group.append(name, binary(type.getSlice(block, position)));
        }
    }

    private static byte[] toFixedBytes(BigInteger value)
    {
        byte[] bytes = value.toByteArray();
        byte[] result = new byte[16];
        Arrays.fill(result, value.signum() < 0 ? (byte) 0xFF : 0);
        System.arraycopy(bytes, 0, result, 16 - bytes.length, bytes.length);
        return result;
    }

    private static Binary binary(Slice slice)
    {
        return Binary.fromConstantByteArray(slice.getBytes());
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
                List<Slice> names = FIELD_NAMES.stream()
                        .map(Slices::utf8Slice)
                        .sorted()
                        .filter(_ -> random.nextBoolean())
                        .collect(toCollection(ArrayList::new));
                // a shredded object has at least one field
                if (names.isEmpty()) {
                    names.add(utf8Slice(FIELD_NAMES.get(random.nextInt(FIELD_NAMES.size()))));
                }
                yield new ObjectSchema(names.stream()
                        .map(name -> new Field(name, randomGroup(random, depth + 1)))
                        .toList());
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
                if (random.nextBoolean()) {
                    fields.put(utf8Slice("extra"), randomValue(random, depth + 1));
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

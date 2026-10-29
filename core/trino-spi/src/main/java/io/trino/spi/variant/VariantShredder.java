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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.variant.Header.PrimitiveType;
import io.trino.spi.variant.VariantDecoder.ObjectLayout;
import io.trino.spi.variant.VariantShreddingSchema.ArraySchema;
import io.trino.spi.variant.VariantShreddingSchema.ObjectSchema;
import io.trino.spi.variant.VariantShreddingSchema.Primitive;
import io.trino.spi.variant.VariantShreddingSchema.ValueGroup;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.variant.VariantEncoder.encodeObject;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/// Splits variant-encoded values into a residual value and a typed value, as described by the
/// [Parquet variant shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md).
///
/// A value is stored in the typed value only if its variant type exactly matches the shredded type. Other values are
/// stored in the residual value. Objects are shredded field by field, and the fields that are not shredded remain in
/// the residual object, which uses the original metadata.
public final class VariantShredder
{
    private VariantShredder() {}

    /// Shreds the variants in the block using the schema. The block may be a dictionary or run-length encoded block.
    /// If the block is already shredded using the schema, it is returned unchanged.
    public static VariantBlock shred(Block block, VariantShreddingSchema schema)
    {
        requireNonNull(schema, "schema is null");
        if (block.getUnderlyingValueBlock() instanceof VariantBlock variantBlock && block == variantBlock && variantBlock.getShreddingSchema().equals(Optional.of(schema))) {
            return variantBlock;
        }

        int positionCount = block.getPositionCount();
        VariableWidthBlockBuilder metadata = new VariableWidthBlockBuilder(null, positionCount, 0);
        VariableWidthBlockBuilder residual = new VariableWidthBlockBuilder(null, positionCount, 0);
        TypedBuilder typed = TypedBuilder.create(schema, positionCount);
        boolean[] isNull = new boolean[positionCount];
        boolean hasNull = false;
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                isNull[position] = true;
                hasNull = true;
                metadata.appendNull();
                residual.appendNull();
                typed.appendNull();
                continue;
            }
            VariantBlock variantBlock = (VariantBlock) block.getUnderlyingValueBlock();
            Variant variant = variantBlock.getVariant(block.getUnderlyingValuePosition(position));
            metadata.writeEntry(VARBINARY.getSlice(variantBlock.getRawMetadata(), variantBlock.getRawOffset() + block.getUnderlyingValuePosition(position)));
            Slice residualValue = typed.append(variant);
            if (residualValue == null) {
                residual.appendNull();
            }
            else {
                residual.writeEntry(residualValue);
            }
        }
        return VariantBlock.createShredded(
                positionCount,
                metadata.build(),
                residual.build(),
                schema,
                typed.build(),
                hasNull ? Optional.of(toValidity(isNull, positionCount)) : Optional.empty());
    }

    private static long[] toValidity(boolean[] isNull, int positionCount)
    {
        long[] valueIsValid = new long[(positionCount + 63) / 64];
        for (int position = 0; position < positionCount; position++) {
            if (!isNull[position]) {
                valueIsValid[position >>> 6] |= 1L << (position & 63);
            }
        }
        return valueIsValid;
    }

    private abstract static class TypedBuilder
    {
        static TypedBuilder create(VariantShreddingSchema schema, int expectedPositions)
        {
            return switch (schema) {
                case Primitive primitive -> new PrimitiveBuilder(primitive.type(), expectedPositions);
                case ObjectSchema objectSchema -> new ObjectBuilder(objectSchema, expectedPositions);
                case ArraySchema arraySchema -> new ArrayBuilder(arraySchema, expectedPositions);
            };
        }

        protected boolean[] isNull;
        protected int positionCount;

        TypedBuilder(int expectedPositions)
        {
            this.isNull = new boolean[max(expectedPositions, 1)];
        }

        protected void addPosition(boolean isNullPosition)
        {
            if (positionCount == isNull.length) {
                isNull = Arrays.copyOf(isNull, positionCount * 2);
            }
            isNull[positionCount] = isNullPosition;
            positionCount++;
        }

        protected Optional<long[]> validity()
        {
            for (int position = 0; position < positionCount; position++) {
                if (isNull[position]) {
                    return Optional.of(toValidity(isNull, positionCount));
                }
            }
            return Optional.empty();
        }

        /// Appends a position where the value does not have the shredded type
        abstract void appendNull();

        /// Appends the value. Returns the residual value, or null if the value is fully stored in the typed value.
        abstract Slice append(Variant value);

        abstract Block build();
    }

    private static final class GroupBuilder
    {
        private final VariableWidthBlockBuilder value;
        private final Optional<TypedBuilder> typed;
        private boolean[] isNull;
        private int positionCount;

        GroupBuilder(ValueGroup group, int expectedPositions)
        {
            this.value = new VariableWidthBlockBuilder(null, expectedPositions, 0);
            this.typed = group.typedValue().map(schema -> TypedBuilder.create(schema, expectedPositions));
            this.isNull = new boolean[max(expectedPositions, 1)];
        }

        /// Appends a null group, used when the parent is not present
        void appendNull()
        {
            value.appendNull();
            typed.ifPresent(TypedBuilder::appendNull);
            addPosition(true);
        }

        /// Appends the value, or a missing value if the value is null
        void append(Variant variant)
        {
            if (variant == null) {
                value.appendNull();
                typed.ifPresent(TypedBuilder::appendNull);
            }
            else if (typed.isPresent()) {
                Slice residual = typed.get().append(variant);
                if (residual == null) {
                    value.appendNull();
                }
                else {
                    value.writeEntry(residual);
                }
            }
            else {
                value.writeEntry(variant.data());
            }
            addPosition(false);
        }

        private void addPosition(boolean isNullPosition)
        {
            if (positionCount == isNull.length) {
                isNull = Arrays.copyOf(isNull, positionCount * 2);
            }
            isNull[positionCount] = isNullPosition;
            positionCount++;
        }

        Block build()
        {
            Block[] fields = typed.isPresent() ? new Block[] {value.build(), typed.get().build()} : new Block[] {value.build()};
            boolean hasNull = false;
            for (int position = 0; position < positionCount; position++) {
                hasNull |= isNull[position];
            }
            return RowBlock.fromNotNullSuppressedFieldBlocks(positionCount, hasNull ? Optional.of(toValidity(isNull, positionCount)) : Optional.empty(), fields);
        }
    }

    private static final class PrimitiveBuilder
            extends TypedBuilder
    {
        private final Type type;
        private final PrimitiveType primitiveType;
        private final BlockBuilder builder;

        PrimitiveBuilder(Type type, int expectedPositions)
        {
            super(expectedPositions);
            this.type = type;
            this.primitiveType = VariantShreddingSchema.variantPrimitiveType(type);
            this.builder = type.createBlockBuilder(null, expectedPositions);
        }

        @Override
        void appendNull()
        {
            builder.appendNull();
            addPosition(true);
        }

        @Override
        Slice append(Variant value)
        {
            if (!matches(value)) {
                appendNull();
                return value.data();
            }
            writeValue(value);
            addPosition(false);
            return null;
        }

        private boolean matches(Variant value)
        {
            if (value.basicType() == BasicType.SHORT_STRING) {
                return primitiveType == PrimitiveType.STRING;
            }
            if (value.basicType() != BasicType.PRIMITIVE) {
                return false;
            }
            PrimitiveType valueType = value.primitiveType();
            return switch (primitiveType) {
                case BOOLEAN_TRUE, BOOLEAN_FALSE -> valueType == PrimitiveType.BOOLEAN_TRUE || valueType == PrimitiveType.BOOLEAN_FALSE;
                case DECIMAL4, DECIMAL8, DECIMAL16 -> valueType == primitiveType && decimalFits((DecimalType) type, value);
                default -> valueType == primitiveType;
            };
        }

        private static boolean decimalFits(DecimalType type, Variant value)
        {
            Slice data = value.data();
            if (data.getByte(1) != type.getScale()) {
                return false;
            }
            return switch (value.primitiveType()) {
                case DECIMAL4 -> Math.abs((long) data.getInt(2)) < longTenToNth(type.getPrecision());
                case DECIMAL8 -> {
                    long unscaled = data.getLong(2);
                    yield unscaled != Long.MIN_VALUE && Math.abs(unscaled) < longTenToNth(type.getPrecision());
                }
                case DECIMAL16 -> decimal16(data).toBigInteger().abs().compareTo(BigInteger.TEN.pow(type.getPrecision())) < 0;
                default -> false;
            };
        }

        private static Int128 decimal16(Slice data)
        {
            return Int128.valueOf(data.getLong(10), data.getLong(2));
        }

        private void writeValue(Variant value)
        {
            Slice data = value.data();
            switch (primitiveType) {
                case BOOLEAN_TRUE, BOOLEAN_FALSE -> BOOLEAN.writeBoolean(builder, value.getBoolean());
                case INT8 -> type.writeLong(builder, value.getByte());
                case INT16 -> type.writeLong(builder, value.getShort());
                case INT32 -> type.writeLong(builder, value.getInt());
                case INT64 -> type.writeLong(builder, value.getLong());
                case FLOAT -> type.writeLong(builder, floatToRawIntBits(value.getFloat()));
                case DOUBLE -> type.writeDouble(builder, value.getDouble());
                case DECIMAL4 -> type.writeLong(builder, data.getInt(2));
                case DECIMAL8 -> type.writeLong(builder, data.getLong(2));
                case DECIMAL16 -> type.writeObject(builder, decimal16(data));
                case DATE -> type.writeLong(builder, value.getDate());
                case TIME_NTZ_MICROS -> type.writeLong(builder, value.getTimeMicros() * PICOSECONDS_PER_MICROSECOND);
                case TIMESTAMP_NTZ_MICROS -> type.writeLong(builder, value.getTimestampMicros());
                case TIMESTAMP_NTZ_NANOS -> {
                    long nanos = value.getTimestampNanos();
                    type.writeObject(builder, new LongTimestamp(floorDiv(nanos, NANOSECONDS_PER_MICROSECOND), floorMod(nanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND));
                }
                case TIMESTAMP_UTC_MICROS -> {
                    long micros = value.getTimestampMicros();
                    type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                            floorDiv(micros, MICROSECONDS_PER_MILLISECOND),
                            floorMod(micros, MICROSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_MICROSECOND,
                            UTC_KEY));
                }
                case TIMESTAMP_UTC_NANOS -> {
                    long nanos = value.getTimestampNanos();
                    type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                            floorDiv(nanos, NANOSECONDS_PER_MILLISECOND),
                            floorMod(nanos, NANOSECONDS_PER_MILLISECOND) * PICOSECONDS_PER_NANOSECOND,
                            UTC_KEY));
                }
                case BINARY -> type.writeSlice(builder, value.getBinary());
                case STRING -> type.writeSlice(builder, value.getString());
                case UUID -> type.writeSlice(builder, value.getUuidSlice());
                case NULL -> throw new IllegalStateException("Variant null cannot be shredded");
            }
        }

        @Override
        Block build()
        {
            return builder.build();
        }
    }

    private static final class ObjectBuilder
            extends TypedBuilder
    {
        private final ObjectSchema schema;
        private final List<GroupBuilder> fields;

        ObjectBuilder(ObjectSchema schema, int expectedPositions)
        {
            super(expectedPositions);
            this.schema = schema;
            this.fields = schema.fields().stream()
                    .map(field -> new GroupBuilder(field.group(), expectedPositions))
                    .toList();
        }

        @Override
        void appendNull()
        {
            fields.forEach(GroupBuilder::appendNull);
            addPosition(true);
        }

        @Override
        Slice append(Variant value)
        {
            if (value.basicType() != BasicType.OBJECT) {
                appendNull();
                return value.data();
            }

            for (int index = 0; index < fields.size(); index++) {
                fields.get(index).append(value.getObjectField(schema.fields().get(index).name()).orElse(null));
            }
            addPosition(false);
            return residualObject(value);
        }

        /// Returns the object containing the fields that are not shredded, or null if every field is shredded.
        private Slice residualObject(Variant value)
        {
            Slice data = value.data();
            Metadata metadata = value.metadata();
            ObjectLayout layout = (ObjectLayout) VariantDecoder.decode(data, 0);
            int[] residualIndexes = new int[layout.count()];
            int residualCount = 0;
            int maxFieldId = -1;
            int totalLength = 0;
            for (int index = 0; index < layout.count(); index++) {
                int fieldId = layout.fieldId(index);
                if (schema.fieldIndex(metadata.get(fieldId)) < 0) {
                    residualIndexes[residualCount] = index;
                    residualCount++;
                    maxFieldId = max(maxFieldId, fieldId);
                    totalLength += layout.valueLengths()[index];
                }
            }
            if (residualCount == 0) {
                return null;
            }
            if (residualCount == layout.count()) {
                return data;
            }

            int[] indexes = Arrays.copyOf(residualIndexes, residualCount);
            Slice output = Slices.allocate(encodedObjectSize(maxFieldId, residualCount, totalLength));
            encodeObject(
                    residualCount,
                    i -> layout.fieldId(indexes[i]),
                    i -> data.slice(layout.valueStart(indexes[i]), layout.valueLengths()[indexes[i]]),
                    output,
                    0);
            return output;
        }

        @Override
        Block build()
        {
            Block[] fieldBlocks = fields.stream()
                    .map(GroupBuilder::build)
                    .toArray(Block[]::new);
            return RowBlock.fromNotNullSuppressedFieldBlocks(positionCount, validity(), fieldBlocks);
        }
    }

    private static final class ArrayBuilder
            extends TypedBuilder
    {
        private final GroupBuilder elements;
        private int[] offsets;
        private int elementCount;

        ArrayBuilder(ArraySchema schema, int expectedPositions)
        {
            super(expectedPositions);
            this.elements = new GroupBuilder(schema.element(), expectedPositions);
            this.offsets = new int[max(expectedPositions, 1) + 1];
        }

        @Override
        void appendNull()
        {
            addOffset();
            addPosition(true);
        }

        @Override
        Slice append(Variant value)
        {
            if (value.basicType() != BasicType.ARRAY) {
                appendNull();
                return value.data();
            }
            int length = value.getArrayLength();
            for (int index = 0; index < length; index++) {
                elements.append(value.getArrayElement(index));
            }
            elementCount += length;
            addOffset();
            addPosition(false);
            return null;
        }

        private void addOffset()
        {
            if (positionCount + 1 >= offsets.length) {
                offsets = Arrays.copyOf(offsets, offsets.length * 2);
            }
            offsets[positionCount + 1] = elementCount;
        }

        @Override
        Block build()
        {
            return ArrayBlock.fromElementBlock(positionCount, validity(), Arrays.copyOf(offsets, positionCount + 1), elements.build());
        }
    }
}

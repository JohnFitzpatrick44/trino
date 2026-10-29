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
import io.trino.spi.block.RowBlock;
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

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.variant.Header.getBasicType;
import static io.trino.spi.variant.VariantEncoder.ENCODED_BOOLEAN_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_BYTE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DATE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL16_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL4_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL8_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DOUBLE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_FLOAT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_INT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_LONG_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_NULL_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_SHORT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_TIMESTAMP_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_TIME_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_UUID_SIZE;
import static io.trino.spi.variant.VariantEncoder.encodeArrayHeading;
import static io.trino.spi.variant.VariantEncoder.encodeBinary;
import static io.trino.spi.variant.VariantEncoder.encodeBoolean;
import static io.trino.spi.variant.VariantEncoder.encodeByte;
import static io.trino.spi.variant.VariantEncoder.encodeDate;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal16;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal4;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal8;
import static io.trino.spi.variant.VariantEncoder.encodeDouble;
import static io.trino.spi.variant.VariantEncoder.encodeFloat;
import static io.trino.spi.variant.VariantEncoder.encodeInt;
import static io.trino.spi.variant.VariantEncoder.encodeLong;
import static io.trino.spi.variant.VariantEncoder.encodeNull;
import static io.trino.spi.variant.VariantEncoder.encodeObjectHeading;
import static io.trino.spi.variant.VariantEncoder.encodeShort;
import static io.trino.spi.variant.VariantEncoder.encodeString;
import static io.trino.spi.variant.VariantEncoder.encodeTimeMicrosNtz;
import static io.trino.spi.variant.VariantEncoder.encodeTimestampMicrosNtz;
import static io.trino.spi.variant.VariantEncoder.encodeTimestampMicrosUtc;
import static io.trino.spi.variant.VariantEncoder.encodeTimestampNanosNtz;
import static io.trino.spi.variant.VariantEncoder.encodeTimestampNanosUtc;
import static io.trino.spi.variant.VariantEncoder.encodeUuid;
import static io.trino.spi.variant.VariantEncoder.encodedArraySize;
import static io.trino.spi.variant.VariantEncoder.encodedBinarySize;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static io.trino.spi.variant.VariantEncoder.encodedStringSize;
import static java.lang.Math.floorDiv;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/// Reconstructs variant-encoded values from shredded data, following the
/// [Parquet variant shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md#reconstructing-a-shredded-variant).
///
/// Reconstruction uses two passes over the shredded data: the first computes the encoded size and the second
/// writes the encoding directly into the output, so no intermediate values are allocated.
public final class ShreddedVariants
{
    private static final int MISSING = -1;

    private ShreddedVariants() {}

    /// Reusable state for reconstructing many values that may share metadata.
    /// Instances are not thread safe.
    public static final class Reconstructor
    {
        private Slice cachedMetadataSlice;
        private Metadata cachedMetadata;
        private final Map<ObjectSchema, int[]> cachedFieldIds = new IdentityHashMap<>();

        /// Returns the metadata for the slice, reusing the previous metadata when the bytes are the same.
        public Metadata metadata(Slice metadataSlice)
        {
            requireNonNull(metadataSlice, "metadataSlice is null");
            if (cachedMetadataSlice == null || !cachedMetadataSlice.equals(metadataSlice)) {
                cachedMetadataSlice = metadataSlice;
                cachedMetadata = Metadata.from(metadataSlice);
                cachedFieldIds.clear();
            }
            return cachedMetadata;
        }

        private int[] fieldIds(ObjectSchema schema)
        {
            int[] fieldIds = cachedFieldIds.get(schema);
            if (fieldIds == null) {
                fieldIds = new int[schema.fields().size()];
                for (int i = 0; i < fieldIds.length; i++) {
                    fieldIds[i] = cachedMetadata.id(schema.fields().get(i).name());
                }
                cachedFieldIds.put(schema, fieldIds);
            }
            return fieldIds;
        }

        /// Reconstructs the variant at the position. The residual and typed blocks must have the same positions,
        /// and the typed block layout must match the schema. If both the residual and typed value are missing,
        /// a variant null is returned, as required for a top-level variant.
        public Variant reconstruct(Slice metadataSlice, Block residual, VariantShreddingSchema schema, Block typed, int position)
        {
            Metadata metadata = metadata(metadataSlice);
            Slice data = encode(residual, Optional.of(schema), typed, position);
            if (data == null) {
                return Variant.NULL_VALUE;
            }
            return Variant.from(metadata, data);
        }

        /// Encodes the value at the position, or returns null if the value is missing.
        /// [#metadata(Slice)] must have been called with the metadata for the position.
        public Slice encode(Block residual, Optional<VariantShreddingSchema> schema, Block typed, int position)
        {
            int size = encodeGroup(residual, schema, typed, position, null, 0);
            if (size == MISSING) {
                return null;
            }
            Slice output = Slices.allocate(size);
            int written = encodeGroup(residual, schema, typed, position, output, 0);
            VariantUtils.verify(written == size, "Encoded size does not match expected size");
            return output;
        }

        /// Encodes the value at the position into the output, or computes the encoded size when the output is null.
        /// Returns the encoded size, or [#MISSING] if both the residual and typed value are null.
        int encodeGroup(Block residual, Optional<VariantShreddingSchema> schema, Block typed, int position, Slice output, int offset)
        {
            boolean typedPresent = schema.isPresent() && !typed.isNull(position);
            boolean residualPresent = !residual.isNull(position);
            if (typedPresent) {
                return switch (schema.get()) {
                    case Primitive primitive -> {
                        checkNoResidual(residualPresent, "shredded primitive");
                        yield encodePrimitive(primitive.type(), typed, position, output, offset);
                    }
                    case ArraySchema arraySchema -> {
                        checkNoResidual(residualPresent, "shredded array");
                        yield encodeArray(arraySchema, typed, position, output, offset);
                    }
                    case ObjectSchema objectSchema -> encodeObject(objectSchema, residualPresent ? VARBINARY.getSlice(residual, position) : null, typed, position, output, offset);
                };
            }
            if (residualPresent) {
                Slice value = VARBINARY.getSlice(residual, position);
                if (output != null) {
                    output.setBytes(offset, value);
                }
                return value.length();
            }
            return MISSING;
        }

        private int encodeArray(ArraySchema schema, Block typed, int position, Slice output, int offset)
        {
            ArrayBlock arrayBlock = (ArrayBlock) typed.getUnderlyingValueBlock();
            int arrayPosition = typed.getUnderlyingValuePosition(position) + arrayBlock.getOffsetBase();
            int start = arrayBlock.getRawOffsets()[arrayPosition];
            int end = arrayBlock.getRawOffsets()[arrayPosition + 1];
            Block elements = arrayBlock.getRawElementBlock();
            int count = end - start;

            ValueGroup element = schema.element();
            GroupBlocks groups = GroupBlocks.of(elements, element);
            int totalLength = 0;
            for (int i = 0; i < count; i++) {
                totalLength += elementSize(groups, element, start + i);
            }
            if (output == null) {
                return encodedArraySize(count, totalLength);
            }
            int written = encodeArrayHeading(count, index -> elementSize(groups, element, start + index), output, offset);
            for (int i = 0; i < count; i++) {
                written += encodeGroup(groups.value(), element.typedValue(), groups.typed(), groups.position(start + i), output, offset + written);
            }
            return written;
        }

        private int elementSize(GroupBlocks groups, ValueGroup element, int elementPosition)
        {
            int size = encodeGroup(groups.value(), element.typedValue(), groups.typed(), groups.position(elementPosition), null, 0);
            if (size == MISSING) {
                throw invalid("shredded array element is missing");
            }
            return size;
        }

        private int encodeObject(ObjectSchema schema, Slice residual, Block typed, int position, Slice output, int offset)
        {
            RowBlock rowBlock = (RowBlock) typed.getUnderlyingValueBlock();
            int rowPosition = typed.getUnderlyingValuePosition(position) + rowBlock.getOffsetBase();
            int[] shreddedFieldIds = fieldIds(schema);

            ObjectLayout residualLayout = null;
            int residualCount = 0;
            if (residual != null) {
                if (getBasicType(residual.getByte(0)) != BasicType.OBJECT) {
                    throw invalid("partially shredded value must be an object");
                }
                residualLayout = (ObjectLayout) VariantDecoder.decode(residual, 0);
                residualCount = residualLayout.count();
            }

            // merge the shredded and residual fields in field name order
            int fieldCount = 0;
            int maxFieldId = -1;
            int totalLength = 0;
            int shreddedIndex = 0;
            int residualIndex = 0;
            int shreddedFieldCount = schema.fields().size();
            while (shreddedIndex < shreddedFieldCount || residualIndex < residualCount) {
                int compare = compareNextFields(schema, shreddedIndex, residualLayout, residualIndex, residualCount);
                if (compare == 0) {
                    throw invalid("field %s is present in both the shredded and residual value".formatted(schema.fields().get(shreddedIndex).name().toStringUtf8()));
                }
                if (compare < 0) {
                    int size = shreddedFieldSize(schema, rowBlock, rowPosition, shreddedIndex);
                    if (size != MISSING) {
                        int fieldId = shreddedFieldId(schema, shreddedFieldIds, shreddedIndex);
                        fieldCount++;
                        maxFieldId = max(maxFieldId, fieldId);
                        totalLength += size;
                    }
                    shreddedIndex++;
                }
                else {
                    fieldCount++;
                    maxFieldId = max(maxFieldId, residualLayout.fieldId(residualIndex));
                    totalLength += residualLayout.valueLengths()[residualIndex];
                    residualIndex++;
                }
            }

            if (output == null) {
                return encodedObjectSize(maxFieldId, fieldCount, totalLength);
            }

            // collect the field order, so the heading and values can be written in one pass each
            // negative entries are residual field indexes (encoded as -index - 1), non-negative entries are shredded field indexes
            int[] order = new int[fieldCount];
            int[] lengths = new int[fieldCount];
            int[] ids = new int[fieldCount];
            int next = 0;
            shreddedIndex = 0;
            residualIndex = 0;
            while (shreddedIndex < shreddedFieldCount || residualIndex < residualCount) {
                int compare = compareNextFields(schema, shreddedIndex, residualLayout, residualIndex, residualCount);
                if (compare < 0) {
                    int size = shreddedFieldSize(schema, rowBlock, rowPosition, shreddedIndex);
                    if (size != MISSING) {
                        order[next] = shreddedIndex;
                        lengths[next] = size;
                        ids[next] = shreddedFieldIds[shreddedIndex];
                        next++;
                    }
                    shreddedIndex++;
                }
                else {
                    order[next] = -residualIndex - 1;
                    lengths[next] = residualLayout.valueLengths()[residualIndex];
                    ids[next] = residualLayout.fieldId(residualIndex);
                    next++;
                    residualIndex++;
                }
            }

            int written = encodeObjectHeading(fieldCount, index -> ids[index], index -> lengths[index], output, offset);
            for (int i = 0; i < fieldCount; i++) {
                if (order[i] >= 0) {
                    ValueGroup group = schema.fields().get(order[i]).group();
                    GroupBlocks groups = GroupBlocks.of(rowBlock.getRawFieldBlock(order[i]), group);
                    written += encodeGroup(groups.value(), group.typedValue(), groups.typed(), groups.position(rowPosition), output, offset + written);
                }
                else {
                    int index = -order[i] - 1;
                    output.setBytes(offset + written, residual, residualLayout.valueStart(index), lengths[i]);
                    written += lengths[i];
                }
            }
            return written;
        }

        private int compareNextFields(ObjectSchema schema, int shreddedIndex, ObjectLayout residualLayout, int residualIndex, int residualCount)
        {
            if (shreddedIndex >= schema.fields().size()) {
                return 1;
            }
            if (residualIndex >= residualCount) {
                return -1;
            }
            Slice residualName = cachedMetadata.get(residualLayout.fieldId(residualIndex));
            return schema.fields().get(shreddedIndex).name().compareTo(residualName);
        }

        private int shreddedFieldId(ObjectSchema schema, int[] shreddedFieldIds, int shreddedIndex)
        {
            int fieldId = shreddedFieldIds[shreddedIndex];
            if (fieldId < 0) {
                throw invalid("shredded field %s is not present in the variant metadata".formatted(schema.fields().get(shreddedIndex).name().toStringUtf8()));
            }
            return fieldId;
        }

        private int shreddedFieldSize(ObjectSchema schema, RowBlock rowBlock, int rowPosition, int fieldIndex)
        {
            ValueGroup group = schema.fields().get(fieldIndex).group();
            GroupBlocks groups = GroupBlocks.of(rowBlock.getRawFieldBlock(fieldIndex), group);
            int groupPosition = groups.position(rowPosition);
            // both value and typed_value may only be present for a partially shredded object
            if (group.typedValue().isPresent() && !(group.typedValue().get() instanceof ObjectSchema) && !groups.value().isNull(groupPosition) && !groups.typed().isNull(groupPosition)) {
                throw invalid("field %s has both a value and a typed_value".formatted(schema.fields().get(fieldIndex).name().toStringUtf8()));
            }
            return encodeGroup(groups.value(), group.typedValue(), groups.typed(), groupPosition, null, 0);
        }
    }

    /// The residual and typed blocks of a value group, aligned with the raw positions of the group block.
    record GroupBlocks(RowBlock rowBlock, Block groupBlock, Block value, Block typed)
    {
        static GroupBlocks of(Block groupBlock, ValueGroup group)
        {
            RowBlock rowBlock = (RowBlock) groupBlock.getUnderlyingValueBlock();
            Block value = rowBlock.getRawFieldBlock(0);
            Block typed = group.typedValue().isPresent() ? rowBlock.getRawFieldBlock(1) : null;
            return new GroupBlocks(rowBlock, groupBlock, value, typed);
        }

        int position(int groupPosition)
        {
            return groupBlock.getUnderlyingValuePosition(groupPosition) + rowBlock.getOffsetBase();
        }
    }

    /// Returns the basic type of the shredded value at the position, without reconstructing the value.
    public static BasicType basicType(Block residual, VariantShreddingSchema schema, Block typed, int position)
    {
        if (!typed.isNull(position)) {
            return switch (schema) {
                case ObjectSchema _ -> BasicType.OBJECT;
                case ArraySchema _ -> BasicType.ARRAY;
                case Primitive primitive -> {
                    if (primitive.primitiveType() == PrimitiveType.STRING && primitive.type().getSlice(typed, position).length() <= Header.SHORT_STRING_MAX_LENGTH) {
                        yield BasicType.SHORT_STRING;
                    }
                    yield BasicType.PRIMITIVE;
                }
            };
        }
        if (!residual.isNull(position)) {
            return getBasicType(VARBINARY.getSlice(residual, position).getByte(0));
        }
        // missing values are variant null
        return BasicType.PRIMITIVE;
    }

    private static int encodePrimitive(Type type, Block block, int position, Slice output, int offset)
    {
        PrimitiveType primitiveType = VariantShreddingSchema.variantPrimitiveType(type);
        return switch (primitiveType) {
            case BOOLEAN_TRUE, BOOLEAN_FALSE -> {
                if (output != null) {
                    encodeBoolean(BOOLEAN.getBoolean(block, position), output, offset);
                }
                yield ENCODED_BOOLEAN_SIZE;
            }
            case INT8 -> {
                if (output != null) {
                    encodeByte((byte) type.getLong(block, position), output, offset);
                }
                yield ENCODED_BYTE_SIZE;
            }
            case INT16 -> {
                if (output != null) {
                    encodeShort((short) type.getLong(block, position), output, offset);
                }
                yield ENCODED_SHORT_SIZE;
            }
            case INT32 -> {
                if (output != null) {
                    encodeInt(toIntExact(type.getLong(block, position)), output, offset);
                }
                yield ENCODED_INT_SIZE;
            }
            case INT64 -> {
                if (output != null) {
                    encodeLong(type.getLong(block, position), output, offset);
                }
                yield ENCODED_LONG_SIZE;
            }
            case FLOAT -> {
                if (output != null) {
                    encodeFloat(REAL.getFloat(block, position), output, offset);
                }
                yield ENCODED_FLOAT_SIZE;
            }
            case DOUBLE -> {
                if (output != null) {
                    encodeDouble(DOUBLE.getDouble(block, position), output, offset);
                }
                yield ENCODED_DOUBLE_SIZE;
            }
            case DECIMAL4 -> {
                if (output != null) {
                    encodeDecimal4(toIntExact(type.getLong(block, position)), ((DecimalType) type).getScale(), output, offset);
                }
                yield ENCODED_DECIMAL4_SIZE;
            }
            case DECIMAL8 -> {
                if (output != null) {
                    encodeDecimal8(type.getLong(block, position), ((DecimalType) type).getScale(), output, offset);
                }
                yield ENCODED_DECIMAL8_SIZE;
            }
            case DECIMAL16 -> {
                if (output != null) {
                    encodeDecimal16((Int128) type.getObject(block, position), ((DecimalType) type).getScale(), output, offset);
                }
                yield ENCODED_DECIMAL16_SIZE;
            }
            case DATE -> {
                if (output != null) {
                    encodeDate(toIntExact(DATE.getLong(block, position)), output, offset);
                }
                yield ENCODED_DATE_SIZE;
            }
            case TIME_NTZ_MICROS -> {
                if (output != null) {
                    encodeTimeMicrosNtz(type.getLong(block, position) / PICOSECONDS_PER_MICROSECOND, output, offset);
                }
                yield ENCODED_TIME_SIZE;
            }
            case TIMESTAMP_NTZ_MICROS -> {
                if (output != null) {
                    encodeTimestampMicrosNtz(type.getLong(block, position), output, offset);
                }
                yield ENCODED_TIMESTAMP_SIZE;
            }
            case TIMESTAMP_NTZ_NANOS -> {
                if (output != null) {
                    LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
                    long nanos = timestamp.getEpochMicros() * NANOSECONDS_PER_MICROSECOND + timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND;
                    encodeTimestampNanosNtz(nanos, output, offset);
                }
                yield ENCODED_TIMESTAMP_SIZE;
            }
            case TIMESTAMP_UTC_MICROS -> {
                if (output != null) {
                    LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);
                    long micros = timestamp.getEpochMillis() * MICROSECONDS_PER_MILLISECOND + floorDiv(timestamp.getPicosOfMilli(), PICOSECONDS_PER_MICROSECOND);
                    encodeTimestampMicrosUtc(micros, output, offset);
                }
                yield ENCODED_TIMESTAMP_SIZE;
            }
            case TIMESTAMP_UTC_NANOS -> {
                if (output != null) {
                    LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);
                    long nanos = timestamp.getEpochMillis() * NANOSECONDS_PER_MILLISECOND + floorDiv(timestamp.getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND);
                    encodeTimestampNanosUtc(nanos, output, offset);
                }
                yield ENCODED_TIMESTAMP_SIZE;
            }
            case BINARY -> {
                Slice value = type.getSlice(block, position);
                if (output != null) {
                    encodeBinary(value, output, offset);
                }
                yield encodedBinarySize(value.length());
            }
            case STRING -> {
                Slice value = type.getSlice(block, position);
                if (output != null) {
                    encodeString(value, output, offset);
                }
                yield encodedStringSize(value.length());
            }
            case UUID -> {
                if (output != null) {
                    encodeUuid(type.getSlice(block, position), output, offset);
                }
                yield ENCODED_UUID_SIZE;
            }
            case NULL -> {
                if (output != null) {
                    encodeNull(output, offset);
                }
                yield ENCODED_NULL_SIZE;
            }
        };
    }

    private static void checkNoResidual(boolean residualPresent, String kind)
    {
        if (residualPresent) {
            throw invalid(kind + " must not have a residual value");
        }
    }

    private static IllegalArgumentException invalid(String message)
    {
        return new IllegalArgumentException("Invalid shredded variant: " + message);
    }
}

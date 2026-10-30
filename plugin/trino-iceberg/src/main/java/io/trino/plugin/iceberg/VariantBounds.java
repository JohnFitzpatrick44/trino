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
package io.trino.plugin.iceberg;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.variant.Header.PrimitiveType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import jakarta.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;

/// Reads the bounds of the values in a variant from Iceberg file metrics.
///
/// The bounds of a variant column are an encoded variant object. The object keys are the normalized JSON paths
/// of fields, and the values are the bounds of the field. Bounds are only written for fields with values of a
/// single variant type, other than null.
final class VariantBounds
{
    private VariantBounds() {}

    /// Returns the domain of the values of [IcebergColumnHandle#isVariantExtract()] column in a file. The domain
    /// only contains a range if the bounds have a type that is cast to the column type without changing the order.
    static Domain variantExtractDomain(IcebergColumnHandle column, @Nullable ByteBuffer lowerBounds, @Nullable ByteBuffer upperBounds)
    {
        Type type = column.getType();
        if (lowerBounds == null || upperBounds == null) {
            return Domain.all(type);
        }
        Optional<String> path = normalizedPath(column.getVariantPaths().getFirst());
        if (path.isEmpty()) {
            return Domain.all(type);
        }
        Optional<Variant> lower = bound(lowerBounds, path.get());
        Optional<Variant> upper = bound(upperBounds, path.get());
        if (lower.isEmpty() || upper.isEmpty()) {
            return Domain.all(type);
        }
        Optional<Object> low = toTrinoValue(type, lower.get());
        Optional<Object> high = toTrinoValue(type, upper.get());
        if (low.isEmpty() || high.isEmpty()) {
            return Domain.all(type);
        }
        // the extracted value is null when the field is missing
        return Domain.create(ValueSet.ofRanges(Range.range(type, low.get(), true, high.get(), true)), true);
    }

    private static Optional<Variant> bound(ByteBuffer bounds, String path)
    {
        Slice slice = Slices.wrappedHeapBuffer(bounds.duplicate());
        int metadataLength = metadataLength(slice);
        Variant object = Variant.from(Metadata.from(slice.slice(0, metadataLength)), slice.slice(metadataLength, slice.length() - metadataLength));
        if (object.basicType() != BasicType.OBJECT) {
            return Optional.empty();
        }
        return object.getObjectField(utf8Slice(path));
    }

    private static int metadataLength(Slice metadata)
    {
        int offsetSize = ((metadata.getByte(0) & 0b1100_0000) >>> 6) + 1;
        int dictionarySize = readOffset(metadata, 1, offsetSize);
        int offsetsStart = 1 + offsetSize;
        int dictionaryStart = offsetsStart + (dictionarySize + 1) * offsetSize;
        return dictionaryStart + readOffset(metadata, offsetsStart + dictionarySize * offsetSize, offsetSize);
    }

    private static int readOffset(Slice slice, int position, int size)
    {
        int value = 0;
        for (int i = 0; i < size; i++) {
            value |= (slice.getByte(position + i) & 0xFF) << (8 * i);
        }
        return value;
    }

    /// Returns the normalized JSON path of the names, or empty if a name needs to be escaped
    private static Optional<String> normalizedPath(List<String> names)
    {
        StringBuilder path = new StringBuilder("$");
        for (String name : names) {
            if (name.chars().anyMatch(character -> character < 0x20 || character == '\'' || character == '\\')) {
                return Optional.empty();
            }
            path.append("['").append(name).append("']");
        }
        return Optional.of(path.toString());
    }

    /// Converts a bound to the type, if the cast of every value of the variant type to the type preserves the order
    private static Optional<Object> toTrinoValue(Type type, Variant bound)
    {
        if (bound.basicType() == BasicType.SHORT_STRING || bound.primitiveType() == PrimitiveType.STRING) {
            return type.equals(VARCHAR) ? Optional.of(bound.getString()) : Optional.empty();
        }
        if (bound.basicType() != BasicType.PRIMITIVE) {
            return Optional.empty();
        }
        PrimitiveType primitiveType = bound.primitiveType();
        if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
            Optional<Long> value = switch (primitiveType) {
                case INT8 -> Optional.of((long) bound.getByte());
                case INT16 -> Optional.of((long) bound.getShort());
                case INT32 -> Optional.of((long) bound.getInt());
                case INT64 -> Optional.of(bound.getLong());
                default -> Optional.empty();
            };
            // values outside the range of the type fail the cast, so the bound is limited to the range
            return value.map(longValue -> clamp(type, longValue));
        }
        if (type.equals(DOUBLE)) {
            return switch (primitiveType) {
                case DOUBLE -> Optional.of(bound.getDouble());
                case FLOAT -> Optional.of((double) bound.getFloat());
                default -> Optional.empty();
            };
        }
        if (type.equals(REAL)) {
            return primitiveType == PrimitiveType.FLOAT ? Optional.of((long) floatToRawIntBits(bound.getFloat())) : Optional.empty();
        }
        if (type.equals(BOOLEAN)) {
            return switch (primitiveType) {
                case BOOLEAN_TRUE, BOOLEAN_FALSE -> Optional.of(bound.getBoolean());
                default -> Optional.empty();
            };
        }
        if (type.equals(DATE)) {
            return primitiveType == PrimitiveType.DATE ? Optional.of((long) bound.getDate()) : Optional.empty();
        }
        if (type instanceof DecimalType decimalType) {
            if (primitiveType != PrimitiveType.DECIMAL4 && primitiveType != PrimitiveType.DECIMAL8 && primitiveType != PrimitiveType.DECIMAL16) {
                return Optional.empty();
            }
            BigDecimal value = bound.getDecimal();
            if (value.scale() != decimalType.getScale() || value.precision() > decimalType.getPrecision()) {
                return Optional.empty();
            }
            if (decimalType.isShort()) {
                return Optional.of(value.unscaledValue().longValueExact());
            }
            return Optional.of(Int128.valueOf(value.unscaledValue()));
        }
        return Optional.empty();
    }

    private static Object clamp(Type type, long value)
    {
        long minimum;
        long maximum;
        if (type.equals(TINYINT)) {
            minimum = Byte.MIN_VALUE;
            maximum = Byte.MAX_VALUE;
        }
        else if (type.equals(SMALLINT)) {
            minimum = Short.MIN_VALUE;
            maximum = Short.MAX_VALUE;
        }
        else if (type.equals(INTEGER)) {
            minimum = Integer.MIN_VALUE;
            maximum = Integer.MAX_VALUE;
        }
        else {
            minimum = Long.MIN_VALUE;
            maximum = Long.MAX_VALUE;
        }
        return max(minimum, min(maximum, value));
    }
}

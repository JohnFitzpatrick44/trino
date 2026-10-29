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
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Header.PrimitiveType;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.variant.VariantUtils.checkArgument;
import static java.util.Objects.requireNonNull;

/// Describes the layout of the shredded part of a variant, as defined by the
/// [Parquet variant shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md).
///
/// A shredded variant is stored as a residual variant-encoded `value` plus a `typed_value`
/// whose layout is described by this schema:
///
/// - [Primitive]: `typed_value` is a block of the primitive Trino type
/// - [ObjectSchema]: `typed_value` is a `RowBlock` with one field per shredded object field;
///   each field is a [ValueGroup] block
/// - [ArraySchema]: `typed_value` is an `ArrayBlock` of [ValueGroup] blocks
///
/// A [ValueGroup] is stored as a `RowBlock` whose first field is the residual variant-encoded
/// `value` (`VARBINARY`) and whose optional second field is the `typed_value`.
///
/// A null `typed_value` means that the value at that position is not of the shredded type.
public sealed interface VariantShreddingSchema
        permits VariantShreddingSchema.ArraySchema,
                VariantShreddingSchema.ObjectSchema,
                VariantShreddingSchema.Primitive
{
    /// A primitive shredded value. The Trino type determines the variant primitive type.
    record Primitive(Type type)
            implements VariantShreddingSchema
    {
        public Primitive
        {
            requireNonNull(type, "type is null");
            // validate the type is supported
            variantPrimitiveType(type);
        }

        public PrimitiveType primitiveType()
        {
            return variantPrimitiveType(type);
        }
    }

    /// A shredded object. Fields are sorted by name using unsigned byte order and are unique.
    record ObjectSchema(List<Field> fields)
            implements VariantShreddingSchema
    {
        public ObjectSchema
        {
            fields = List.copyOf(requireNonNull(fields, "fields is null"));
            for (int i = 1; i < fields.size(); i++) {
                Slice previous = fields.get(i - 1).name();
                Slice current = fields.get(i).name();
                checkArgument(previous.compareTo(current) < 0, () -> "Shredded object fields must be sorted and unique: %s, %s".formatted(previous.toStringUtf8(), current.toStringUtf8()));
            }
        }

        /// Returns the index of the named field, or -1 if the field is not shredded.
        public int fieldIndex(Slice name)
        {
            int low = 0;
            int high = fields.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                int compare = fields.get(mid).name().compareTo(name);
                if (compare < 0) {
                    low = mid + 1;
                }
                else if (compare > 0) {
                    high = mid - 1;
                }
                else {
                    return mid;
                }
            }
            return -1;
        }
    }

    /// A shredded array. Every element is stored in a [ValueGroup].
    record ArraySchema(ValueGroup element)
            implements VariantShreddingSchema
    {
        public ArraySchema
        {
            requireNonNull(element, "element is null");
        }
    }

    /// A shredded object field.
    record Field(Slice name, ValueGroup group)
    {
        public Field
        {
            requireNonNull(name, "name is null");
            requireNonNull(group, "group is null");
        }
    }

    /// The `value` and optional `typed_value` pair used for shredded object fields and array elements.
    /// The residual `value` is always present in the block layout; it is entirely null when the file
    /// does not store it.
    record ValueGroup(Optional<VariantShreddingSchema> typedValue)
    {
        public ValueGroup
        {
            requireNonNull(typedValue, "typedValue is null");
        }

        public static ValueGroup valueOnly()
        {
            return new ValueGroup(Optional.empty());
        }

        public static ValueGroup typed(VariantShreddingSchema typedValue)
        {
            return new ValueGroup(Optional.of(typedValue));
        }
    }

    /// Returns the variant primitive type used to represent values of the Trino type.
    ///
    /// @throws IllegalArgumentException if the Trino type cannot be a shredded primitive
    static PrimitiveType variantPrimitiveType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            // the shredded column does not distinguish true and false
            return PrimitiveType.BOOLEAN_TRUE;
        }
        if (type.equals(TINYINT)) {
            return PrimitiveType.INT8;
        }
        if (type.equals(SMALLINT)) {
            return PrimitiveType.INT16;
        }
        if (type.equals(INTEGER)) {
            return PrimitiveType.INT32;
        }
        if (type.equals(BIGINT)) {
            return PrimitiveType.INT64;
        }
        if (type.equals(REAL)) {
            return PrimitiveType.FLOAT;
        }
        if (type.equals(DOUBLE)) {
            return PrimitiveType.DOUBLE;
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.getPrecision() <= 9) {
                return PrimitiveType.DECIMAL4;
            }
            if (decimalType.getPrecision() <= 18) {
                return PrimitiveType.DECIMAL8;
            }
            return PrimitiveType.DECIMAL16;
        }
        if (type.equals(DATE)) {
            return PrimitiveType.DATE;
        }
        if (type.equals(TimeType.TIME_MICROS)) {
            return PrimitiveType.TIME_NTZ_MICROS;
        }
        if (type.equals(TimestampType.TIMESTAMP_MICROS)) {
            return PrimitiveType.TIMESTAMP_NTZ_MICROS;
        }
        if (type.equals(TimestampType.TIMESTAMP_NANOS)) {
            return PrimitiveType.TIMESTAMP_NTZ_NANOS;
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS)) {
            return PrimitiveType.TIMESTAMP_UTC_MICROS;
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS)) {
            return PrimitiveType.TIMESTAMP_UTC_NANOS;
        }
        if (type.equals(VARBINARY)) {
            return PrimitiveType.BINARY;
        }
        if (type.equals(VARCHAR)) {
            return PrimitiveType.STRING;
        }
        if (type.equals(UUID)) {
            return PrimitiveType.UUID;
        }
        throw new IllegalArgumentException("Unsupported shredded variant type: " + type);
    }
}

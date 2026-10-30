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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.VariantFields;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

/// Computes the Iceberg bounds of the shredded fields of the VARIANT columns of a Parquet file.
///
/// The bounds of a variant are a variant object, keyed by the normalized JSON path of each field, of the lower or
/// upper bounds of the field. The bounds of a field are only written when every value of the field is stored in
/// the shredded column, which means that all the values have the same variant type.
final class ParquetVariantMetrics
{
    private ParquetVariantMetrics() {}

    static void addVariantBounds(
            List<BlockMetadata> blocks,
            MessageType fileType,
            Schema icebergSchema,
            MetricsConfig metricsConfig,
            Map<Integer, ByteBuffer> lowerBounds,
            Map<Integer, ByteBuffer> upperBounds)
    {
        for (org.apache.parquet.schema.Type field : fileType.getFields()) {
            if (field.getId() == null || field.isPrimitive() || !field.asGroupType().containsField(VariantFields.TYPED_VALUE)) {
                continue;
            }
            int fieldId = field.getId().intValue();
            NestedField icebergField = icebergSchema.findField(fieldId);
            if (icebergField == null || !icebergField.type().isVariantType()) {
                continue;
            }
            MetricsMode metricsMode = MetricsUtil.metricsMode(icebergSchema, metricsConfig, fieldId);
            if (metricsMode == MetricsModes.None.get() || metricsMode == MetricsModes.Counts.get()) {
                continue;
            }

            Map<Slice, Variant> lower = new LinkedHashMap<>();
            Map<Slice, Variant> upper = new LinkedHashMap<>();
            List<ShreddedLeaf> leaves = new ArrayList<>();
            collectLeaves(field.asGroupType(), ImmutableList.of(field.getName()), ImmutableList.of(), leaves);
            for (ShreddedLeaf leaf : leaves) {
                Optional<String> path = normalizedPath(leaf.names());
                if (path.isEmpty()) {
                    continue;
                }
                fieldBounds(blocks, leaf, metricsMode).ifPresent(bounds -> {
                    lower.put(utf8Slice(path.get()), bounds.lower());
                    upper.put(utf8Slice(path.get()), bounds.upper());
                });
            }
            if (!lower.isEmpty()) {
                lowerBounds.put(fieldId, serialize(Variant.ofObject(lower)));
                upperBounds.put(fieldId, serialize(Variant.ofObject(upper)));
            }
        }
    }

    /// Collects the shredded primitive fields of objects, which are not within an array
    private static void collectLeaves(GroupType valueGroup, List<String> columnPath, List<String> names, List<ShreddedLeaf> leaves)
    {
        if (!valueGroup.containsField(VariantFields.TYPED_VALUE)) {
            return;
        }
        org.apache.parquet.schema.Type typedValue = valueGroup.getType(VariantFields.TYPED_VALUE);
        List<String> typedValuePath = ImmutableList.<String>builder().addAll(columnPath).add(VariantFields.TYPED_VALUE).build();
        if (typedValue.isPrimitive()) {
            if (!names.isEmpty()) {
                leaves.add(new ShreddedLeaf(names, typedValuePath, ImmutableList.<String>builder().addAll(columnPath).add(VariantFields.VALUE).build(), typedValue.asPrimitiveType()));
            }
            return;
        }
        if (typedValue.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
            return;
        }
        for (org.apache.parquet.schema.Type fieldGroup : typedValue.asGroupType().getFields()) {
            if (!fieldGroup.isPrimitive()) {
                collectLeaves(
                        fieldGroup.asGroupType(),
                        ImmutableList.<String>builder().addAll(typedValuePath).add(fieldGroup.getName()).build(),
                        ImmutableList.<String>builder().addAll(names).add(fieldGroup.getName()).build(),
                        leaves);
            }
        }
    }

    private static Optional<Bounds> fieldBounds(List<BlockMetadata> blocks, ShreddedLeaf leaf, MetricsMode metricsMode)
    {
        Comparator<Object> comparator = leafComparator(leaf.type());
        Object minimum = null;
        Object maximum = null;
        for (BlockMetadata block : blocks) {
            for (ColumnChunkMetadata column : block.columns()) {
                List<String> path = Arrays.asList(column.getPath().toArray());
                Statistics<?> statistics = column.getStatistics();
                if (path.equals(leaf.valuePath())) {
                    // a value that is not stored in the shredded column has a different type
                    if (statistics == null || statistics.isEmpty() || statistics.getNumNulls() != column.getValueCount()) {
                        return Optional.empty();
                    }
                }
                else if (path.equals(leaf.typedValuePath())) {
                    if (statistics == null || statistics.isEmpty()) {
                        return Optional.empty();
                    }
                    if (statistics.hasNonNullValue()) {
                        Object columnMinimum = statistics.genericGetMin();
                        Object columnMaximum = statistics.genericGetMax();
                        if (minimum == null || comparator.compare(columnMinimum, minimum) < 0) {
                            minimum = columnMinimum;
                        }
                        if (maximum == null || comparator.compare(columnMaximum, maximum) > 0) {
                            maximum = columnMaximum;
                        }
                    }
                }
            }
        }
        if (minimum == null) {
            return Optional.empty();
        }
        Optional<Variant> lower = toVariant(leaf.type(), minimum, metricsMode, true);
        Optional<Variant> upper = toVariant(leaf.type(), maximum, metricsMode, false);
        if (lower.isEmpty() || upper.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Bounds(lower.get(), upper.get()));
    }

    @SuppressWarnings("unchecked")
    private static Comparator<Object> leafComparator(PrimitiveType type)
    {
        Comparator<?> comparator = type.comparator();
        return (Comparator<Object>) comparator;
    }

    private static Optional<Variant> toVariant(PrimitiveType parquetType, Object value, MetricsMode metricsMode, boolean lower)
    {
        Type type = VariantFields.shreddedPrimitiveType(parquetType);
        if (type.equals(BOOLEAN)) {
            return Optional.of(Variant.ofBoolean((Boolean) value));
        }
        if (type.equals(TINYINT)) {
            return Optional.of(Variant.ofByte(((Integer) value).byteValue()));
        }
        if (type.equals(SMALLINT)) {
            return Optional.of(Variant.ofShort(((Integer) value).shortValue()));
        }
        if (type.equals(INTEGER)) {
            return Optional.of(Variant.ofInt((Integer) value));
        }
        if (type.equals(BIGINT)) {
            return Optional.of(Variant.ofLong((Long) value));
        }
        if (type.equals(DATE)) {
            return Optional.of(Variant.ofDate((Integer) value));
        }
        if (type.equals(REAL)) {
            float floatValue = (Float) value;
            return Float.isNaN(floatValue) ? Optional.empty() : Optional.of(Variant.ofFloat(floatValue));
        }
        if (type.equals(DOUBLE)) {
            double doubleValue = (Double) value;
            return Double.isNaN(doubleValue) ? Optional.empty() : Optional.of(Variant.ofDouble(doubleValue));
        }
        if (type instanceof DecimalType decimalType) {
            BigInteger unscaled = switch (value) {
                case Integer intValue -> BigInteger.valueOf(intValue);
                case Long longValue -> BigInteger.valueOf(longValue);
                case Binary binary -> new BigInteger(binary.getBytes());
                default -> throw new IllegalArgumentException("Unexpected decimal statistics: " + value);
            };
            return Optional.of(Variant.ofDecimal(new BigDecimal(unscaled, decimalType.getScale())));
        }
        if (type.equals(VARCHAR)) {
            String string = ((Binary) value).toStringUsingUTF8();
            if (metricsMode instanceof MetricsModes.Truncate truncate && string.codePointCount(0, string.length()) > truncate.length()) {
                if (!lower) {
                    // the truncated upper bound would need to be incremented
                    return Optional.empty();
                }
                string = string.substring(0, string.offsetByCodePoints(0, truncate.length()));
            }
            return Optional.of(Variant.ofString(string));
        }
        // other types are not ordered the same way in Parquet and Iceberg, or are not used for filtering
        return Optional.empty();
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

    private static ByteBuffer serialize(Variant variant)
    {
        Slice metadata = variant.metadata().toSlice();
        Slice value = variant.data();
        Slice serialized = Slices.allocate(metadata.length() + value.length());
        serialized.setBytes(0, metadata);
        serialized.setBytes(metadata.length(), value);
        return serialized.toByteBuffer();
    }

    private record ShreddedLeaf(List<String> names, List<String> typedValuePath, List<String> valuePath, PrimitiveType type) {}

    private record Bounds(Variant lower, Variant upper) {}
}

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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.variant.Header.PrimitiveType;
import io.trino.spi.variant.ObjectFieldIdValue;
import io.trino.spi.variant.Variant;
import io.trino.spi.variant.VariantShreddingSchema;
import io.trino.spi.variant.VariantShreddingSchema.ArraySchema;
import io.trino.spi.variant.VariantShreddingSchema.Field;
import io.trino.spi.variant.VariantShreddingSchema.ObjectSchema;
import io.trino.spi.variant.VariantShreddingSchema.Primitive;
import io.trino.spi.variant.VariantShreddingSchema.ValueGroup;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
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
import static java.lang.Math.max;

/// Infers a shredding schema for variants from sample values, using the rules of the Iceberg variant shredding analyzer:
///
/// - A value is shredded only if all the values observed at its path have the same type family, ignoring variant nulls.
///   Integers of different widths and decimals of different widths with the same scale are in the same family, and the
///   widest type is used. Values with a narrower type are stored in the residual value, because values are shredded only
///   when the type matches exactly.
/// - Object fields that are present in less than 10% of the objects are not shredded, and at most the 300 most frequent
///   fields of an object are shredded, preferring the fields that are first in name order.
/// - Values are not shredded below a depth of 50, and at most 1000 distinct fields are tracked for an object.
///
/// The inferred schema only depends on the values, not on their order, as long as an object has at most 1000 distinct fields.
public final class VariantShreddingInference
{
    private static final double MIN_FIELD_FREQUENCY = 0.10;
    private static final int MAX_SHREDDED_FIELDS = 300;
    private static final int MAX_SHREDDING_DEPTH = 50;
    private static final int MAX_TRACKED_FIELDS = 1000;

    private VariantShreddingInference() {}

    /// Returns the shredding schema for the variants in the blocks, or empty if the variants should not be shredded.
    public static Optional<VariantShreddingSchema> inferShreddingSchema(List<Block> blocks)
    {
        // if every block is already shredded with the same schema, the schema is used as is
        Optional<VariantShreddingSchema> inputSchema = commonShreddingSchema(blocks);
        if (inputSchema.isPresent()) {
            return inputSchema;
        }

        Node root = new Node();
        for (Block block : blocks) {
            VariantBlock variantBlock = (VariantBlock) block.getUnderlyingValueBlock();
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    root.observe(variantBlock.getVariant(block.getUnderlyingValuePosition(position)), 0);
                }
            }
        }
        return root.schema(0);
    }

    private static Optional<VariantShreddingSchema> commonShreddingSchema(List<Block> blocks)
    {
        Optional<VariantShreddingSchema> schema = Optional.empty();
        for (Block block : blocks) {
            if (!(block.getUnderlyingValueBlock() instanceof VariantBlock variantBlock) || variantBlock.getShreddingSchema().isEmpty()) {
                return Optional.empty();
            }
            if (schema.isPresent() && !schema.equals(variantBlock.getShreddingSchema())) {
                return Optional.empty();
            }
            schema = variantBlock.getShreddingSchema();
        }
        return schema;
    }

    private enum Family
    {
        BOOLEAN,
        INTEGER,
        DECIMAL,
        FLOAT,
        DOUBLE,
        DATE,
        TIME,
        TIMESTAMP_UTC_MICROS,
        TIMESTAMP_UTC_NANOS,
        TIMESTAMP_NTZ_MICROS,
        TIMESTAMP_NTZ_NANOS,
        BINARY,
        STRING,
        UUID,
        OBJECT,
        ARRAY,
    }

    private static final class Node
    {
        private final Map<Family, Integer> familyCounts = new EnumMap<>(Family.class);
        // widest integer type, as the number of bytes
        private int integerBytes;
        // widest decimal type, as the number of bytes, and the scale
        private int decimalBytes;
        private int decimalScale = -1;
        private boolean mixedDecimalScales;
        private final Map<Slice, Node> fields = new HashMap<>();
        private Node element;

        void observe(Variant value, int depth)
        {
            if (value.isNull()) {
                return;
            }
            Family family = family(value);
            familyCounts.merge(family, 1, Integer::sum);
            if (depth >= MAX_SHREDDING_DEPTH) {
                return;
            }
            switch (family) {
                case INTEGER -> integerBytes = max(integerBytes, switch (value.primitiveType()) {
                    case INT8 -> 1;
                    case INT16 -> 2;
                    case INT32 -> 4;
                    default -> 8;
                });
                case DECIMAL -> {
                    decimalBytes = max(decimalBytes, switch (value.primitiveType()) {
                        case DECIMAL4 -> 4;
                        case DECIMAL8 -> 8;
                        default -> 16;
                    });
                    int scale = value.data().getByte(1);
                    if (decimalScale >= 0 && decimalScale != scale) {
                        mixedDecimalScales = true;
                    }
                    decimalScale = scale;
                }
                case OBJECT -> value.objectFields().forEach(field -> observeField(value, field, depth));
                case ARRAY -> {
                    if (element == null) {
                        element = new Node();
                    }
                    for (int index = 0; index < value.getArrayLength(); index++) {
                        element.observe(value.getArrayElement(index), depth + 1);
                    }
                }
                default -> {}
            }
        }

        private void observeField(Variant object, ObjectFieldIdValue field, int depth)
        {
            Slice name = object.metadata().get(field.fieldId());
            Node child = fields.get(name);
            if (child == null) {
                if (fields.size() >= MAX_TRACKED_FIELDS) {
                    return;
                }
                child = new Node();
                fields.put(name.copy(), child);
            }
            child.observe(field.value(), depth + 1);
        }

        int observations()
        {
            return familyCounts.values().stream().mapToInt(Integer::intValue).sum();
        }

        Optional<VariantShreddingSchema> schema(int depth)
        {
            if (familyCounts.size() != 1 || depth >= MAX_SHREDDING_DEPTH) {
                return Optional.empty();
            }
            Family family = familyCounts.keySet().iterator().next();
            return switch (family) {
                case OBJECT -> objectSchema(depth);
                case ARRAY -> Optional.of(new ArraySchema(valueGroup(element, depth + 1)));
                case DECIMAL -> {
                    if (mixedDecimalScales) {
                        yield Optional.empty();
                    }
                    int precision = switch (decimalBytes) {
                        case 4 -> 9;
                        case 8 -> 18;
                        default -> 38;
                    };
                    if (decimalScale > precision) {
                        yield Optional.empty();
                    }
                    yield Optional.of(new Primitive(DecimalType.createDecimalType(precision, decimalScale)));
                }
                default -> Optional.of(new Primitive(primitiveType(family)));
            };
        }

        private Optional<VariantShreddingSchema> objectSchema(int depth)
        {
            int objects = familyCounts.get(Family.OBJECT);
            List<Map.Entry<Slice, Node>> frequentFields = fields.entrySet().stream()
                    .filter(entry -> entry.getValue().observations() >= MIN_FIELD_FREQUENCY * objects)
                    .sorted(Comparator.<Map.Entry<Slice, Node>>comparingInt(entry -> entry.getValue().observations()).reversed()
                            .thenComparing(Map.Entry::getKey))
                    .limit(MAX_SHREDDED_FIELDS)
                    .sorted(Map.Entry.comparingByKey())
                    .collect(toImmutableList());
            if (frequentFields.isEmpty()) {
                return Optional.empty();
            }
            List<Field> shreddedFields = new ArrayList<>();
            for (Map.Entry<Slice, Node> entry : frequentFields) {
                shreddedFields.add(new Field(entry.getKey(), valueGroup(entry.getValue(), depth + 1)));
            }
            return Optional.of(new ObjectSchema(ImmutableList.copyOf(shreddedFields)));
        }

        private static ValueGroup valueGroup(Node node, int depth)
        {
            if (node == null) {
                return ValueGroup.valueOnly();
            }
            return node.schema(depth)
                    .map(ValueGroup::typed)
                    .orElseGet(ValueGroup::valueOnly);
        }

        private Type primitiveType(Family family)
        {
            return switch (family) {
                case BOOLEAN -> BOOLEAN;
                case INTEGER -> switch (integerBytes) {
                    case 1 -> TINYINT;
                    case 2 -> SMALLINT;
                    case 4 -> INTEGER;
                    default -> BIGINT;
                };
                case FLOAT -> REAL;
                case DOUBLE -> DOUBLE;
                case DATE -> DATE;
                case TIME -> TIME_MICROS;
                case TIMESTAMP_UTC_MICROS -> TIMESTAMP_TZ_MICROS;
                case TIMESTAMP_UTC_NANOS -> TIMESTAMP_TZ_NANOS;
                case TIMESTAMP_NTZ_MICROS -> TIMESTAMP_MICROS;
                case TIMESTAMP_NTZ_NANOS -> TIMESTAMP_NANOS;
                case BINARY -> VARBINARY;
                case STRING -> VARCHAR;
                case UUID -> UUID;
                case DECIMAL, OBJECT, ARRAY -> throw new IllegalArgumentException("Not a primitive family: " + family);
            };
        }

        private static Family family(Variant value)
        {
            if (value.basicType() == BasicType.SHORT_STRING) {
                return Family.STRING;
            }
            if (value.basicType() == BasicType.OBJECT) {
                return Family.OBJECT;
            }
            if (value.basicType() == BasicType.ARRAY) {
                return Family.ARRAY;
            }
            PrimitiveType primitiveType = value.primitiveType();
            return switch (primitiveType) {
                case BOOLEAN_TRUE, BOOLEAN_FALSE -> Family.BOOLEAN;
                case INT8, INT16, INT32, INT64 -> Family.INTEGER;
                case DECIMAL4, DECIMAL8, DECIMAL16 -> Family.DECIMAL;
                case FLOAT -> Family.FLOAT;
                case DOUBLE -> Family.DOUBLE;
                case DATE -> Family.DATE;
                case TIME_NTZ_MICROS -> Family.TIME;
                case TIMESTAMP_UTC_MICROS -> Family.TIMESTAMP_UTC_MICROS;
                case TIMESTAMP_UTC_NANOS -> Family.TIMESTAMP_UTC_NANOS;
                case TIMESTAMP_NTZ_MICROS -> Family.TIMESTAMP_NTZ_MICROS;
                case TIMESTAMP_NTZ_NANOS -> Family.TIMESTAMP_NTZ_NANOS;
                case BINARY -> Family.BINARY;
                case STRING -> Family.STRING;
                case UUID -> Family.UUID;
                case NULL -> throw new IllegalArgumentException("Variant null does not have a family");
            };
        }
    }
}

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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.parquet.VariantField.TypedValue;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Header;
import io.trino.spi.variant.VariantShreddingSchema;
import io.trino.spi.variant.VariantShreddingSchema.ArraySchema;
import io.trino.spi.variant.VariantShreddingSchema.ObjectSchema;
import io.trino.spi.variant.VariantShreddingSchema.Primitive;
import io.trino.spi.variant.VariantShreddingSchema.ValueGroup;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
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
import static java.lang.String.format;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

/// Builds the reader fields for Parquet VARIANT groups, including shredded variants as described by the
/// [Parquet variant shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md).
public final class VariantFields
{
    public static final String METADATA = "metadata";
    public static final String VALUE = "value";
    public static final String TYPED_VALUE = "typed_value";

    private VariantFields() {}

    /// Returns true if the group has the layout of a VARIANT group: a `metadata` field and a `value` or `typed_value` field.
    public static boolean isVariantGroup(GroupColumnIO group)
    {
        return group.getChild(METADATA) != null && (group.getChild(VALUE) != null || group.getChild(TYPED_VALUE) != null);
    }

    /// Returns true if the VARIANT group is shredded.
    public static boolean isShredded(GroupColumnIO group)
    {
        return group.getChild(TYPED_VALUE) != null;
    }

    /// Builds the field for a VARIANT group. The group may contain a subset of the fields stored in the file,
    /// in which case the shredding schema of the field only describes the fields that are present.
    public static VariantField constructVariantField(Type type, GroupColumnIO variant)
    {
        PrimitiveColumnIO metadataColumn = primitiveChild(variant, METADATA)
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "VARIANT column is missing the metadata field: " + variant.getType()));
        PrimitiveField metadata = new PrimitiveField(VARBINARY, metadataColumn.getType().getRepetition() != OPTIONAL, metadataColumn.getColumnDescriptor(), metadataColumn.getId());
        Optional<Field> value = primitiveChild(variant, VALUE)
                .map(column -> new PrimitiveField(VARBINARY, column.getType().getRepetition() != OPTIONAL, column.getColumnDescriptor(), column.getId()));
        Optional<TypedValue> typedValue = Optional.ofNullable(variant.getChild(TYPED_VALUE)).map(VariantFields::constructTypedValue);
        if (value.isEmpty() && typedValue.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "VARIANT column must have a value or typed_value field: " + variant.getType());
        }
        return new VariantField(
                type,
                variant.getRepetitionLevel(),
                variant.getDefinitionLevel(),
                variant.getType().getRepetition() != OPTIONAL,
                value,
                metadata,
                typedValue);
    }

    private static Optional<PrimitiveColumnIO> primitiveChild(GroupColumnIO group, String name)
    {
        ColumnIO child = group.getChild(name);
        if (child == null) {
            return Optional.empty();
        }
        if (!(child instanceof PrimitiveColumnIO primitive) || primitive.getType().asPrimitiveType().getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.BINARY) {
            throw new TrinoException(NOT_SUPPORTED, format("VARIANT field %s must be binary: %s", name, group.getType()));
        }
        return Optional.of(primitive);
    }

    private static TypedValue constructTypedValue(ColumnIO typedValue)
    {
        if (typedValue instanceof PrimitiveColumnIO primitive) {
            Type type = shreddedPrimitiveType(primitive.getType().asPrimitiveType());
            return new TypedValue(new Primitive(type), new PrimitiveField(type, false, primitive.getColumnDescriptor(), primitive.getId()));
        }

        GroupColumnIO group = (GroupColumnIO) typedValue;
        if (group.getType().getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
            return constructArray(group);
        }
        return constructObject(group);
    }

    private static TypedValue constructArray(GroupColumnIO list)
    {
        // the specification requires a 3-level list with a required element group
        if (list.getChildrenCount() != 1 ||
                !(list.getChild(0) instanceof GroupColumnIO repeated) ||
                !repeated.getType().isRepetition(REPEATED) ||
                repeated.getChildrenCount() != 1 ||
                !(repeated.getChild(0) instanceof GroupColumnIO elementGroup)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported shredded VARIANT array layout: " + list.getType());
        }
        ShreddedGroup element = constructValueGroup(elementGroup);
        GroupField field = new GroupField(
                new ArrayType(element.field().getType()),
                list.getRepetitionLevel(),
                list.getDefinitionLevel(),
                list.getType().getRepetition() != OPTIONAL,
                ImmutableList.of(Optional.of(element.field())));
        return new TypedValue(new ArraySchema(element.group()), field);
    }

    private static TypedValue constructObject(GroupColumnIO object)
    {
        List<NamedGroup> fields = new ArrayList<>();
        for (int i = 0; i < object.getChildrenCount(); i++) {
            ColumnIO child = object.getChild(i);
            if (!(child instanceof GroupColumnIO fieldGroup)) {
                throw new TrinoException(NOT_SUPPORTED, "Shredded VARIANT object field must be a group: " + child.getType());
            }
            fields.add(new NamedGroup(utf8Slice(child.getName()), constructValueGroup(fieldGroup)));
        }
        // the shredding schema requires fields in name order, using unsigned byte order
        fields.sort(Comparator.comparing(NamedGroup::name));

        RowType rowType = RowType.anonymous(fields.stream()
                .map(field -> field.group().field().getType())
                .collect(toImmutableList()));
        GroupField field = new GroupField(
                rowType,
                object.getRepetitionLevel(),
                object.getDefinitionLevel(),
                object.getType().getRepetition() != OPTIONAL,
                fields.stream()
                        .map(namedGroup -> Optional.<Field>of(namedGroup.group().field()))
                        .collect(toImmutableList()));
        VariantShreddingSchema schema = new ObjectSchema(fields.stream()
                .map(namedGroup -> new VariantShreddingSchema.Field(namedGroup.name(), namedGroup.group().group()))
                .collect(toImmutableList()));
        return new TypedValue(schema, field);
    }

    private static ShreddedGroup constructValueGroup(GroupColumnIO group)
    {
        Optional<Field> value = primitiveChild(group, VALUE)
                .map(column -> new PrimitiveField(VARBINARY, false, column.getColumnDescriptor(), column.getId()));
        Optional<TypedValue> typedValue = Optional.ofNullable(group.getChild(TYPED_VALUE)).map(VariantFields::constructTypedValue);
        if (value.isEmpty() && typedValue.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Shredded VARIANT group must have a value or typed_value field: " + group.getType());
        }

        ImmutableList.Builder<Type> types = ImmutableList.<Type>builder().add(VARBINARY);
        ImmutableList.Builder<Optional<Field>> children = ImmutableList.<Optional<Field>>builder().add(value);
        typedValue.ifPresent(typed -> {
            types.add(typed.field().getType());
            children.add(Optional.of(typed.field()));
        });
        GroupField field = new GroupField(
                RowType.anonymous(types.build()),
                group.getRepetitionLevel(),
                group.getDefinitionLevel(),
                group.getType().getRepetition() != OPTIONAL,
                children.build());
        return new ShreddedGroup(new ValueGroup(typedValue.map(TypedValue::schema)), field);
    }

    /// Returns the Trino type used to read a shredded primitive, as defined in the shredding specification.
    public static Type shreddedPrimitiveType(PrimitiveType primitiveType)
    {
        LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();
        Optional<Type> type = switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN -> annotation == null ? Optional.of(BOOLEAN) : Optional.empty();
            case INT32 -> switch (annotation) {
                case null -> Optional.of(INTEGER);
                case IntLogicalTypeAnnotation intAnnotation when intAnnotation.isSigned() -> switch (intAnnotation.getBitWidth()) {
                    case 8 -> Optional.of(TINYINT);
                    case 16 -> Optional.of(SMALLINT);
                    case 32 -> Optional.of(INTEGER);
                    default -> Optional.empty();
                };
                case DecimalLogicalTypeAnnotation decimal -> Optional.of(DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale()));
                case LogicalTypeAnnotation.DateLogicalTypeAnnotation _ -> Optional.of(DATE);
                default -> Optional.empty();
            };
            case INT64 -> switch (annotation) {
                case null -> Optional.of(BIGINT);
                case IntLogicalTypeAnnotation intAnnotation when intAnnotation.isSigned() && intAnnotation.getBitWidth() == 64 -> Optional.of(BIGINT);
                case DecimalLogicalTypeAnnotation decimal -> Optional.of(DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale()));
                case TimeLogicalTypeAnnotation time when time.getUnit() == TimeUnit.MICROS && !time.isAdjustedToUTC() -> Optional.of(TIME_MICROS);
                case TimestampLogicalTypeAnnotation timestamp when timestamp.getUnit() == TimeUnit.MICROS -> Optional.of(timestamp.isAdjustedToUTC() ? TIMESTAMP_TZ_MICROS : TIMESTAMP_MICROS);
                case TimestampLogicalTypeAnnotation timestamp when timestamp.getUnit() == TimeUnit.NANOS -> Optional.of(timestamp.isAdjustedToUTC() ? TIMESTAMP_TZ_NANOS : TIMESTAMP_NANOS);
                default -> Optional.empty();
            };
            case FLOAT -> annotation == null ? Optional.of(REAL) : Optional.empty();
            case DOUBLE -> annotation == null ? Optional.of(DOUBLE) : Optional.empty();
            case BINARY -> switch (annotation) {
                case null -> Optional.of(VARBINARY);
                case StringLogicalTypeAnnotation _ -> Optional.of(VARCHAR);
                case DecimalLogicalTypeAnnotation decimal -> Optional.of(DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale()));
                default -> Optional.empty();
            };
            case FIXED_LEN_BYTE_ARRAY -> switch (annotation) {
                case UUIDLogicalTypeAnnotation _ when primitiveType.getTypeLength() == 16 -> Optional.of(UUID);
                case DecimalLogicalTypeAnnotation decimal -> Optional.of(DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale()));
                case null, default -> Optional.empty();
            };
            case INT96 -> Optional.empty();
        };
        return type.orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported shredded VARIANT type: " + primitiveType));
    }

    /// Returns the shredding schema of a `typed_value` field written with [#toParquetType].
    /// Object fields must be in name order and value groups must store `value` before `typed_value`,
    /// which is the layout of the typed value blocks.
    public static VariantShreddingSchema shreddingSchema(org.apache.parquet.schema.Type typedValue)
    {
        if (typedValue.isPrimitive()) {
            return new Primitive(shreddedPrimitiveType(typedValue.asPrimitiveType()));
        }
        GroupType group = typedValue.asGroupType();
        if (group.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
            GroupType repeated = group.getType(0).asGroupType();
            return new ArraySchema(valueGroupSchema(repeated.getType(0).asGroupType()));
        }
        return new ObjectSchema(group.getFields().stream()
                .map(field -> new VariantShreddingSchema.Field(utf8Slice(field.getName()), valueGroupSchema(field.asGroupType())))
                .collect(toImmutableList()));
    }

    private static ValueGroup valueGroupSchema(GroupType group)
    {
        if (group.getFieldCount() == 1 && group.getFieldName(0).equals(VALUE)) {
            return ValueGroup.valueOnly();
        }
        if (group.getFieldCount() != 2 || !group.getFieldName(0).equals(VALUE) || !group.getFieldName(1).equals(TYPED_VALUE)) {
            throw new IllegalArgumentException("Shredded VARIANT group must contain value and typed_value fields: " + group);
        }
        return ValueGroup.typed(shreddingSchema(group.getType(1)));
    }

    /// Returns the Parquet type of a VARIANT group with the shredding schema.
    public static GroupType toParquetType(String name, Repetition repetition, Optional<VariantShreddingSchema> schema)
    {
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition)
                .as(LogicalTypeAnnotation.variantType(Header.VERSION))
                .required(BINARY).named(METADATA);
        if (schema.isEmpty()) {
            return builder.required(BINARY).named(VALUE).named(name);
        }
        return builder
                .optional(BINARY).named(VALUE)
                .addField(toParquetType(TYPED_VALUE, schema.get()))
                .named(name);
    }

    private static org.apache.parquet.schema.Type toParquetType(String name, VariantShreddingSchema schema)
    {
        return switch (schema) {
            case Primitive primitive -> toParquetPrimitive(primitive.type()).named(name);
            case ObjectSchema objectSchema -> {
                if (objectSchema.fields().isEmpty()) {
                    throw new IllegalArgumentException("Shredded VARIANT object must have at least one field");
                }
                Types.GroupBuilder<GroupType> builder = Types.optionalGroup();
                for (VariantShreddingSchema.Field field : objectSchema.fields()) {
                    builder.addField(toParquetValueGroup(Repetition.REQUIRED, field.name().toStringUtf8(), field.group()));
                }
                yield builder.named(name);
            }
            case ArraySchema arraySchema -> Types.optionalGroup()
                    .as(LogicalTypeAnnotation.listType())
                    .addField(Types.repeatedGroup()
                            .addField(toParquetValueGroup(Repetition.REQUIRED, "element", arraySchema.element()))
                            .named("list"))
                    .named(name);
        };
    }

    private static GroupType toParquetValueGroup(Repetition repetition, String name, ValueGroup group)
    {
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition)
                .optional(BINARY).named(VALUE);
        group.typedValue().ifPresent(typedValue -> builder.addField(toParquetType(TYPED_VALUE, typedValue)));
        return builder.named(name);
    }

    private static Types.PrimitiveBuilder<PrimitiveType> toParquetPrimitive(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN);
        }
        if (type.equals(TINYINT)) {
            return Types.optional(INT32).as(LogicalTypeAnnotation.intType(8, true));
        }
        if (type.equals(SMALLINT)) {
            return Types.optional(INT32).as(LogicalTypeAnnotation.intType(16, true));
        }
        if (type.equals(INTEGER)) {
            return Types.optional(INT32);
        }
        if (type.equals(BIGINT)) {
            return Types.optional(INT64);
        }
        if (type.equals(REAL)) {
            return Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT);
        }
        if (type.equals(DOUBLE)) {
            return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE);
        }
        if (type instanceof DecimalType decimalType) {
            LogicalTypeAnnotation annotation = LogicalTypeAnnotation.decimalType(decimalType.getScale(), decimalType.getPrecision());
            if (decimalType.getPrecision() <= 9) {
                return Types.optional(INT32).as(annotation);
            }
            if (decimalType.getPrecision() <= 18) {
                return Types.optional(INT64).as(annotation);
            }
            return Types.optional(FIXED_LEN_BYTE_ARRAY).length(16).as(annotation);
        }
        if (type.equals(DATE)) {
            return Types.optional(INT32).as(LogicalTypeAnnotation.dateType());
        }
        if (type.equals(TIME_MICROS)) {
            return Types.optional(INT64).as(LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS));
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return Types.optional(INT64).as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS));
        }
        if (type.equals(TIMESTAMP_NANOS)) {
            return Types.optional(INT64).as(LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS));
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return Types.optional(INT64).as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS));
        }
        if (type.equals(TIMESTAMP_TZ_NANOS)) {
            return Types.optional(INT64).as(LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS));
        }
        if (type.equals(VARBINARY)) {
            return Types.optional(BINARY);
        }
        if (type.equals(VARCHAR)) {
            return Types.optional(BINARY).as(LogicalTypeAnnotation.stringType());
        }
        if (type.equals(UUID)) {
            return Types.optional(FIXED_LEN_BYTE_ARRAY).length(16).as(LogicalTypeAnnotation.uuidType());
        }
        throw new IllegalArgumentException("Unsupported shredded VARIANT type: " + type);
    }

    private record ShreddedGroup(ValueGroup group, GroupField field) {}

    private record NamedGroup(Slice name, ShreddedGroup group) {}
}

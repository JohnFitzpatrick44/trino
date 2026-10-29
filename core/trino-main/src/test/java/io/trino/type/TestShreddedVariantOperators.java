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
package io.trino.type;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.block.VariantBlockBuilder;
import io.trino.spi.variant.Variant;
import io.trino.spi.variant.VariantShredder;
import io.trino.spi.variant.VariantShreddingSchema;
import io.trino.spi.variant.VariantShreddingSchema.ArraySchema;
import io.trino.spi.variant.VariantShreddingSchema.Field;
import io.trino.spi.variant.VariantShreddingSchema.ObjectSchema;
import io.trino.spi.variant.VariantShreddingSchema.Primitive;
import io.trino.spi.variant.VariantShreddingSchema.ValueGroup;
import io.trino.type.VariantOperators.VariantArraySubscript;
import io.trino.type.VariantOperators.VariantObjectSubscript;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies that the subscripts read from shredded blocks behave exactly like the subscripts on variant values
final class TestShreddedVariantOperators
{
    private static final List<Variant> VALUES = Arrays.asList(
            object("id", Variant.ofLong(1), "name", Variant.ofString("a"), "tags", array(Variant.ofString("x"), Variant.ofLong(2))),
            object("id", Variant.ofString("not a number"), "other", Variant.ofBoolean(true)),
            object("name", Variant.NULL_VALUE, "nested", object("id", Variant.ofLong(3))),
            object("tags", Variant.ofString("not an array")),
            Variant.EMPTY_OBJECT,
            Variant.ofString("not an object"),
            Variant.NULL_VALUE,
            array(Variant.ofLong(7), object("id", Variant.ofLong(8))),
            Variant.EMPTY_ARRAY);

    private static final List<VariantShreddingSchema> SCHEMAS = ImmutableList.of(
            new ObjectSchema(ImmutableList.of(
                    new Field(utf8Slice("id"), ValueGroup.typed(new Primitive(BIGINT))),
                    new Field(utf8Slice("tags"), ValueGroup.typed(new ArraySchema(ValueGroup.typed(new Primitive(VARCHAR))))))),
            new ObjectSchema(ImmutableList.of(
                    new Field(utf8Slice("name"), ValueGroup.valueOnly()),
                    new Field(utf8Slice("nested"), ValueGroup.typed(new ObjectSchema(ImmutableList.of(new Field(utf8Slice("id"), ValueGroup.typed(new Primitive(BIGINT))))))))),
            new ArraySchema(ValueGroup.typed(new ObjectSchema(ImmutableList.of(new Field(utf8Slice("id"), ValueGroup.typed(new Primitive(BIGINT))))))),
            new Primitive(VARCHAR));

    @Test
    void testObjectSubscript()
    {
        for (VariantShreddingSchema schema : SCHEMAS) {
            VariantBlock block = VariantShredder.shred(createBlock(VALUES), schema);
            for (int position = 0; position < VALUES.size(); position++) {
                for (String field : ImmutableList.of("id", "name", "tags", "nested", "other", "missing")) {
                    Variant value = VALUES.get(position);
                    Slice fieldName = utf8Slice(field);
                    int blockPosition = position;
                    assertSameResult(
                            () -> VariantObjectSubscript.dereference(value, fieldName),
                            () -> VariantObjectSubscript.dereference(block, blockPosition, fieldName));
                }
            }
        }
    }

    @Test
    void testArraySubscript()
    {
        for (VariantShreddingSchema schema : SCHEMAS) {
            VariantBlock block = VariantShredder.shred(createBlock(VALUES), schema);
            for (int position = 0; position < VALUES.size(); position++) {
                for (long index = -1; index <= 3; index++) {
                    Variant value = VALUES.get(position);
                    long arrayIndex = index;
                    int blockPosition = position;
                    assertSameResult(
                            () -> VariantArraySubscript.dereference(value, arrayIndex),
                            () -> VariantArraySubscript.dereference(block, blockPosition, arrayIndex));
                }
            }
        }
    }

    private static void assertSameResult(Supplier<Variant> expected, Supplier<Variant> actual)
    {
        Variant expectedValue;
        try {
            expectedValue = expected.get();
        }
        catch (RuntimeException e) {
            assertThatThrownBy(actual::get)
                    .isInstanceOf(e.getClass())
                    .hasMessage(e.getMessage());
            return;
        }
        Variant actualValue = actual.get();
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        }
        else {
            assertThat(actualValue.toObject()).isEqualTo(expectedValue.toObject());
        }
    }

    private static VariantBlock createBlock(List<Variant> values)
    {
        VariantBlockBuilder builder = VARIANT.createBlockBuilder(null, values.size());
        values.forEach(builder::writeEntry);
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

    private static Variant array(Variant... elements)
    {
        return Variant.ofArray(ImmutableList.copyOf(elements));
    }
}

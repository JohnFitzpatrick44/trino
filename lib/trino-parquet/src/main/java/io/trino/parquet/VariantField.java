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

import io.trino.spi.type.Type;
import io.trino.spi.variant.VariantShreddingSchema;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class VariantField
        extends Field
{
    private final Optional<Field> value;
    private final Field metadata;
    private final Optional<TypedValue> typedValue;

    public VariantField(Type type, int repetitionLevel, int definitionLevel, boolean required, Field value, Field metadata)
    {
        this(type, repetitionLevel, definitionLevel, required, Optional.of(value), metadata, Optional.empty());
    }

    public VariantField(Type type, int repetitionLevel, int definitionLevel, boolean required, Optional<Field> value, Field metadata, Optional<TypedValue> typedValue)
    {
        super(type, repetitionLevel, definitionLevel, required);
        this.value = requireNonNull(value, "value is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typedValue = requireNonNull(typedValue, "typedValue is null");
        checkArgument(value.isPresent() || typedValue.isPresent(), "VARIANT field must have a value or a typed value");
    }

    /// Returns the variant-encoded value field.
    ///
    /// @throws IllegalStateException if the file does not store the value field
    public Field getValue()
    {
        return value.orElseThrow(() -> new IllegalStateException("VARIANT field does not have a value field"));
    }

    public Optional<Field> getOptionalValue()
    {
        return value;
    }

    public Field getMetadata()
    {
        return metadata;
    }

    /// Returns the shredded typed value, if the variant is shredded.
    public Optional<TypedValue> getTypedValue()
    {
        return typedValue;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", getType())
                .add("repetitionLevel", getRepetitionLevel())
                .add("definitionLevel", getDefinitionLevel())
                .add("required", isRequired())
                .add("value", value)
                .add("metadata", getMetadata())
                .add("typedValue", typedValue)
                .toString();
    }

    /// The shredded typed value of a variant. The field reads blocks with the layout described by the schema.
    public record TypedValue(VariantShreddingSchema schema, Field field)
    {
        public TypedValue
        {
            requireNonNull(schema, "schema is null");
            requireNonNull(field, "field is null");
        }
    }
}

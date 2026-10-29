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

package io.trino.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.variant.VariantShreddingSchema;
import io.trino.spi.variant.VariantShreddingSchema.ArraySchema;
import io.trino.spi.variant.VariantShreddingSchema.Field;
import io.trino.spi.variant.VariantShreddingSchema.ObjectSchema;
import io.trino.spi.variant.VariantShreddingSchema.Primitive;
import io.trino.spi.variant.VariantShreddingSchema.ValueGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.block.EncoderUtil.decodeValidityAsLongs;
import static io.trino.spi.block.EncoderUtil.encodeValidityAsLongs;

public class VariantBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "VARIANT";

    private static final byte UNSHREDDED = 0;
    private static final byte SHREDDED = 1;

    private static final byte PRIMITIVE_SCHEMA = 0;
    private static final byte OBJECT_SCHEMA = 1;
    private static final byte ARRAY_SCHEMA = 2;

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Class<? extends Block> getBlockClass()
    {
        return VariantBlock.class;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        VariantBlock variantBlock = (VariantBlock) block;

        sliceOutput.appendInt(variantBlock.getPositionCount());

        Optional<VariantShreddingSchema> shreddingSchema = variantBlock.getShreddingSchema();
        sliceOutput.appendByte(shreddingSchema.isPresent() ? SHREDDED : UNSHREDDED);

        blockEncodingSerde.writeBlock(sliceOutput, variantBlock.getMetadata());
        blockEncodingSerde.writeBlock(sliceOutput, variantBlock.getResidualValues());
        if (shreddingSchema.isPresent()) {
            writeSchema(blockEncodingSerde, sliceOutput, shreddingSchema.get());
            blockEncodingSerde.writeBlock(sliceOutput, variantBlock.getTypedValue());
        }

        encodeValidityAsLongs(sliceOutput, variantBlock.getRawValueIsValid(), variantBlock.getOffsetBase(), variantBlock.getPositionCount());
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        byte layout = sliceInput.readByte();

        Block metadataBlock = blockEncodingSerde.readBlock(sliceInput);
        Block valuesBlock = blockEncodingSerde.readBlock(sliceInput);
        VariantShreddingSchema shreddingSchema = null;
        Block typedValue = null;
        if (layout == SHREDDED) {
            shreddingSchema = readSchema(blockEncodingSerde, sliceInput);
            typedValue = blockEncodingSerde.readBlock(sliceInput);
        }
        else if (layout != UNSHREDDED) {
            throw new IllegalArgumentException("Unknown variant block layout: " + layout);
        }

        long[] valueIsValid = decodeValidityAsLongs(sliceInput, positionCount);

        return VariantBlock.createInternal(0, positionCount, valueIsValid, metadataBlock, valuesBlock, shreddingSchema, typedValue);
    }

    private static void writeSchema(BlockEncodingSerde blockEncodingSerde, SliceOutput output, VariantShreddingSchema schema)
    {
        switch (schema) {
            case Primitive primitive -> {
                output.appendByte(PRIMITIVE_SCHEMA);
                blockEncodingSerde.writeType(output, primitive.type());
            }
            case ObjectSchema objectSchema -> {
                output.appendByte(OBJECT_SCHEMA);
                output.appendInt(objectSchema.fields().size());
                for (Field field : objectSchema.fields()) {
                    output.appendInt(field.name().length());
                    output.writeBytes(field.name());
                    writeValueGroup(blockEncodingSerde, output, field.group());
                }
            }
            case ArraySchema arraySchema -> {
                output.appendByte(ARRAY_SCHEMA);
                writeValueGroup(blockEncodingSerde, output, arraySchema.element());
            }
        }
    }

    private static void writeValueGroup(BlockEncodingSerde blockEncodingSerde, SliceOutput output, ValueGroup group)
    {
        output.appendByte(group.typedValue().isPresent() ? 1 : 0);
        group.typedValue().ifPresent(typedValue -> writeSchema(blockEncodingSerde, output, typedValue));
    }

    private static VariantShreddingSchema readSchema(BlockEncodingSerde blockEncodingSerde, SliceInput input)
    {
        byte kind = input.readByte();
        return switch (kind) {
            case PRIMITIVE_SCHEMA -> new Primitive(blockEncodingSerde.readType(input));
            case OBJECT_SCHEMA -> {
                int fieldCount = input.readInt();
                List<Field> fields = new ArrayList<>(fieldCount);
                for (int i = 0; i < fieldCount; i++) {
                    Slice name = input.readSlice(input.readInt()).copy();
                    fields.add(new Field(name, readValueGroup(blockEncodingSerde, input)));
                }
                yield new ObjectSchema(fields);
            }
            case ARRAY_SCHEMA -> new ArraySchema(readValueGroup(blockEncodingSerde, input));
            default -> throw new IllegalArgumentException("Unknown variant shredding schema kind: " + kind);
        };
    }

    private static ValueGroup readValueGroup(BlockEncodingSerde blockEncodingSerde, SliceInput input)
    {
        if (input.readByte() == 0) {
            return ValueGroup.valueOnly();
        }
        return ValueGroup.typed(readSchema(blockEncodingSerde, input));
    }
}

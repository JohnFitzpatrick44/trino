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
import io.trino.spi.variant.Header;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.ShreddedVariants;
import io.trino.spi.variant.ShreddedVariants.Reconstructor;
import io.trino.spi.variant.Variant;
import io.trino.spi.variant.VariantShreddingSchema;
import jakarta.annotation.Nullable;

import java.util.Optional;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.checkBitRange;
import static io.trino.spi.block.Bitmap.compactBitmap;
import static io.trino.spi.block.Bitmap.copyBitmapAndAppendUnset;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidPosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class VariantBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(VariantBlock.class);

    private final int startOffset;
    private final int positionCount;
    @Nullable
    private final long[] valueIsValid;
    /**
     * Metadata and value blocks have the same position count as this variant block. The field value of a null variant must be null.
     * For a shredded variant, the value block contains the residual variant-encoded values, which are null where the value is
     * fully represented by the typed value.
     */
    private final Block metadata;
    private final Block values;
    @Nullable
    private final VariantShreddingSchema shreddingSchema;
    @Nullable
    private final Block typedValue;

    private volatile long sizeInBytes = -1;
    private volatile long retainedSizeInBytes = -1;

    /**
     * Creates a variant block directly from metadata and value blocks.
     */
    public static VariantBlock create(int positionCount, Block metadata, Block values, Optional<long[]> valueIsValidOptional)
    {
        // verify that field values for null variants are null
        if (valueIsValidOptional.isPresent()) {
            long[] valueIsValid = valueIsValidOptional.get();
            checkBitRange(valueIsValid, 0, positionCount);
            verifyPositionsAreNull(metadata, valueIsValid, positionCount, "Metadata");
            verifyPositionsAreNull(values, valueIsValid, positionCount, "Values");
        }

        return createInternal(0, positionCount, valueIsValidOptional.orElse(null), metadata, values);
    }

    /// Creates a shredded variant block. The residual values and the typed value must have the same position count as
    /// the metadata, and the typed value must use the layout described by the shredding schema. For a null variant,
    /// the metadata, residual value, and typed value must be null.
    public static VariantBlock createShredded(
            int positionCount,
            Block metadata,
            Block residualValues,
            VariantShreddingSchema shreddingSchema,
            Block typedValue,
            Optional<long[]> valueIsValidOptional)
    {
        requireNonNull(shreddingSchema, "shreddingSchema is null");
        requireNonNull(typedValue, "typedValue is null");
        if (valueIsValidOptional.isPresent()) {
            long[] valueIsValid = valueIsValidOptional.get();
            checkBitRange(valueIsValid, 0, positionCount);
            verifyPositionsAreNull(metadata, valueIsValid, positionCount, "Metadata");
            verifyPositionsAreNull(residualValues, valueIsValid, positionCount, "Values");
            verifyPositionsAreNull(typedValue, valueIsValid, positionCount, "Typed value");
        }
        return new VariantBlock(0, positionCount, valueIsValidOptional.orElse(null), metadata, residualValues, shreddingSchema, typedValue);
    }

    private static void verifyPositionsAreNull(Block block, long[] valueIsValid, int positionCount, String name)
    {
        for (int position = 0; position < positionCount; position++) {
            if (!Bitmap.isSet(valueIsValid, 0, position) && !block.isNull(position)) {
                throw new IllegalArgumentException("%s for null variant must be null: position %d".formatted(name, position));
            }
        }
    }

    static VariantBlock createInternal(int startOffset, int positionCount, @Nullable long[] valueIsValid, Block metadata, Block values)
    {
        return new VariantBlock(startOffset, positionCount, valueIsValid, metadata, values, null, null);
    }

    static VariantBlock createInternal(
            int startOffset,
            int positionCount,
            @Nullable long[] valueIsValid,
            Block metadata,
            Block values,
            @Nullable VariantShreddingSchema shreddingSchema,
            @Nullable Block typedValue)
    {
        return new VariantBlock(startOffset, positionCount, valueIsValid, metadata, values, shreddingSchema, typedValue);
    }

    /**
     * Use createInternal or fromMetadataValuesBlocks instead of this method. The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private VariantBlock(
            int startOffset,
            int positionCount,
            @Nullable long[] valueIsValid,
            Block metadata,
            Block values,
            @Nullable VariantShreddingSchema shreddingSchema,
            @Nullable Block typedValue)
    {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        checkBitRange(valueIsValid, startOffset, positionCount);

        requireNonNull(metadata, "metadata is null");
        requireNonNull(values, "values is null");
        if (metadata.getPositionCount() != values.getPositionCount()) {
            throw new IllegalArgumentException("metadata and values blocks must have the same position count");
        }
        if (metadata.getPositionCount() - startOffset < positionCount) {
            throw new IllegalArgumentException("fieldBlock length is less than positionCount");
        }
        if ((shreddingSchema == null) != (typedValue == null)) {
            throw new IllegalArgumentException("shreddingSchema and typedValue must both be present or both be absent");
        }
        if (typedValue != null && typedValue.getPositionCount() != metadata.getPositionCount()) {
            throw new IllegalArgumentException("metadata and typed value blocks must have the same position count");
        }

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.valueIsValid = positionCount == 0 ? null : valueIsValid;
        this.metadata = metadata;
        this.values = values;
        this.shreddingSchema = shreddingSchema;
        this.typedValue = typedValue;
    }

    /// Returns true if this block stores part of the values in shredded typed columns.
    public boolean isShredded()
    {
        return shreddingSchema != null;
    }

    /// Returns the shredding schema of this block, if the block is shredded.
    public Optional<VariantShreddingSchema> getShreddingSchema()
    {
        return Optional.ofNullable(shreddingSchema);
    }

    /// Returns the residual variant-encoded values of this block. For an unshredded block these are the values.
    /// For a shredded block, the residual is null where the value is fully represented by the typed value.
    public Block getResidualValues()
    {
        return region(values);
    }

    /// Returns the typed value of a shredded block.
    ///
    /// @throws IllegalStateException if the block is not shredded
    public Block getTypedValue()
    {
        return region(getRawTypedValue());
    }

    /// Returns the raw typed value of a shredded block, which is indexed by raw position like [#getRawValues()].
    ///
    /// @throws IllegalStateException if the block is not shredded
    public Block getRawTypedValue()
    {
        if (typedValue == null) {
            throw new IllegalStateException("Variant block is not shredded");
        }
        return typedValue;
    }

    private Block region(Block block)
    {
        if ((startOffset == 0) && (block.getPositionCount() == positionCount)) {
            return block;
        }
        return block.getRegion(startOffset, positionCount);
    }

    /// Returns an equivalent block that stores every value in the variant encoding. If the block is
    /// not shredded, this block is returned.
    public VariantBlock unshred()
    {
        if (shreddingSchema == null) {
            return this;
        }
        Reconstructor reconstructor = new Reconstructor();
        long expectedBytes = values.getRegionSizeInBytes(startOffset, positionCount) + typedValue.getRegionSizeInBytes(startOffset, positionCount);
        VariableWidthBlockBuilder valuesBuilder = new VariableWidthBlockBuilder(null, positionCount, toIntExact(min(Integer.MAX_VALUE - 8, expectedBytes)));
        for (int position = 0; position < positionCount; position++) {
            if (isNull(position)) {
                valuesBuilder.appendNull();
                continue;
            }
            int rawPosition = startOffset + position;
            reconstructor.metadata(VARBINARY.getSlice(metadata, rawPosition));
            Slice value = reconstructor.encode(values, Optional.of(shreddingSchema), typedValue, rawPosition);
            valuesBuilder.writeEntry(value == null ? Variant.NULL_VALUE.data() : value);
        }
        return new VariantBlock(0, positionCount, compactBitmap(valueIsValid, startOffset, positionCount), getMetadata(), valuesBuilder.build(), null, null);
    }

    public Block getMetadata()
    {
        if ((startOffset == 0) && (metadata.getPositionCount() == positionCount)) {
            return metadata;
        }
        return metadata.getRegion(startOffset, positionCount);
    }

    /// Returns the variant-encoded values of this block. For a shredded block, the values are reconstructed,
    /// which is expensive; use [#getResidualValues()] and [#getTypedValue()] to access the shredded data directly.
    public Block getValues()
    {
        if (shreddingSchema != null) {
            return unshred().getValues();
        }
        return region(values);
    }

    /// Returns the basic type of the variant value at the specified raw position.
    ///
    /// Unlike [#getVariant(int)] and [#isNull(int)], the position indexes [#getRawValues()], not this block.
    /// To look up a position of this block, add [#getRawOffset()] to it.
    public Header.BasicType getBasicType(int rawPosition)
    {
        if (shreddingSchema != null) {
            return ShreddedVariants.basicType(values, shreddingSchema, typedValue, rawPosition);
        }
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) values.getUnderlyingValueBlock();
        int valuePosition = values.getUnderlyingValuePosition(rawPosition);

        Slice rawSlice = variableWidthBlock.getRawSlice();
        int rawSliceOffset = variableWidthBlock.getRawSliceOffset(valuePosition);
        return Header.getBasicType(rawSlice.getByte(rawSliceOffset));
    }

    public int getRawOffset()
    {
        return startOffset;
    }

    public Block getRawMetadata()
    {
        return metadata;
    }

    /// Returns the raw variant-encoded values. For a shredded block these are the residual values, see [#getResidualValues()].
    public Block getRawValues()
    {
        return values;
    }

    public int getOffsetBase()
    {
        return startOffset;
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsValid != null;
    }

    @Override
    public boolean hasNull()
    {
        return Bitmap.hasUnsetBit(valueIsValid, startOffset, positionCount);
    }

    /// Returns raw validity bitmap words using the [Bitmap] encoding, or null if all positions are valid.
    ///
    /// The returned array is raw block storage. Use [getValidityBitmap()] unless the caller already has the matching
    /// raw bit offset.
    @Nullable
    public long[] getRawValueIsValid()
    {
        return valueIsValid;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes >= 0) {
            return sizeInBytes;
        }

        long sizeInBytes = Byte.BYTES * (long) positionCount;
        sizeInBytes += metadata.getRegionSizeInBytes(startOffset, positionCount);
        sizeInBytes += values.getRegionSizeInBytes(startOffset, positionCount);
        if (typedValue != null) {
            sizeInBytes += typedValue.getRegionSizeInBytes(startOffset, positionCount);
        }
        this.sizeInBytes = sizeInBytes;
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = this.retainedSizeInBytes;
        if (retainedSizeInBytes < 0) {
            retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsValid);
            retainedSizeInBytes += metadata.getRetainedSizeInBytes();
            retainedSizeInBytes += values.getRetainedSizeInBytes();
            if (typedValue != null) {
                retainedSizeInBytes += typedValue.getRetainedSizeInBytes();
            }
            this.retainedSizeInBytes = retainedSizeInBytes;
        }
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(metadata, metadata.getRetainedSizeInBytes());
        consumer.accept(values, values.getRetainedSizeInBytes());
        if (typedValue != null) {
            consumer.accept(typedValue, typedValue.getRetainedSizeInBytes());
        }
        if (valueIsValid != null) {
            consumer.accept(valueIsValid, sizeOf(valueIsValid));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        return "VariantBlock{startOffset=%d, positionCount=%d, shredded=%s}".formatted(startOffset, positionCount, isShredded());
    }

    @Override
    public VariantBlock copyWithAppendedNull()
    {
        long[] newValueIsValid = compactBitmap(copyBitmapAndAppendUnset(valueIsValid, startOffset, positionCount), startOffset, positionCount + 1);
        Block newMetadata = getMetadata().copyWithAppendedNull();
        Block newValues = region(values).copyWithAppendedNull();
        Block newTypedValue = typedValue == null ? null : region(typedValue).copyWithAppendedNull();
        return new VariantBlock(0, positionCount + 1, newValueIsValid, newMetadata, newValues, shreddingSchema, newTypedValue);
    }

    @Override
    public VariantBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        Block newMetadata = copyBlockPositions(positions, offset, length, metadata, startOffset, positionCount);
        Block newValues = copyBlockPositions(positions, offset, length, values, startOffset, positionCount);
        Block newTypedValue = typedValue == null ? null : copyBlockPositions(positions, offset, length, typedValue, startOffset, positionCount);

        long[] newValueIsValid = null;
        if (valueIsValid != null) {
            newValueIsValid = new long[Bitmap.wordsForBits(length)];
            for (int i = 0; i < length; i++) {
                if (Bitmap.isSet(valueIsValid, startOffset, positions[offset + i])) {
                    set(newValueIsValid, 0, i);
                }
            }
            if (!Bitmap.hasUnsetBit(newValueIsValid, 0, length)) {
                newValueIsValid = null;
            }
        }

        return new VariantBlock(0, length, newValueIsValid, newMetadata, newValues, shreddingSchema, newTypedValue);
    }

    private static Block copyBlockPositions(int[] positions, int offset, int length, Block block, int blockOffset, int blockLength)
    {
        // If the variant block has a non-zero starting offset, we have to create a temporary block starting
        // from the correct offset before copying positions
        if (blockOffset != 0) {
            block = block.getRegion(blockOffset, blockLength);
        }
        return block.copyPositions(positions, offset, length);
    }

    @Override
    public VariantBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        return new VariantBlock(startOffset + positionOffset, length, valueIsValid, metadata, values, shreddingSchema, typedValue);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        checkValidRegion(positionCount, position, length);

        long regionSizeInBytes = Byte.BYTES * (long) length;
        regionSizeInBytes += metadata.getRegionSizeInBytes(startOffset + position, length);
        regionSizeInBytes += values.getRegionSizeInBytes(startOffset + position, length);
        if (typedValue != null) {
            regionSizeInBytes += typedValue.getRegionSizeInBytes(startOffset + position, length);
        }
        return regionSizeInBytes;
    }

    @Override
    public VariantBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        Block newMetadata = metadata.copyRegion(startOffset + positionOffset, length);
        Block newValues = values.copyRegion(startOffset + positionOffset, length);
        Block newTypedValue = typedValue == null ? null : typedValue.copyRegion(startOffset + positionOffset, length);

        long[] newValueIsValid = compactBitmap(valueIsValid, startOffset + positionOffset, length);
        if (startOffset == 0 && newValueIsValid == valueIsValid && metadata == newMetadata && values == newValues && typedValue == newTypedValue) {
            return this;
        }
        return new VariantBlock(0, length, newValueIsValid, newMetadata, newValues, shreddingSchema, newTypedValue);
    }

    public Variant getVariant(int position)
    {
        checkValidPosition(position, positionCount);
        if (isNull(position)) {
            throw new IllegalStateException("Position is null");
        }
        int rawPosition = startOffset + position;
        if (shreddingSchema != null) {
            return new Reconstructor().reconstruct(getSlice(metadata, rawPosition), values, shreddingSchema, typedValue, rawPosition);
        }
        return Variant.from(Metadata.from(getSlice(metadata, rawPosition)), getSlice(values, rawPosition));
    }

    private static Slice getSlice(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getSlice(valuePosition);
    }

    @Override
    public VariantBlock getSingleValueBlock(int position)
    {
        checkValidPosition(position, positionCount);

        Block newMetadata = metadata.getSingleValueBlock(startOffset + position);
        Block newValues = values.getSingleValueBlock(startOffset + position);
        Block newTypedValue = typedValue == null ? null : typedValue.getSingleValueBlock(startOffset + position);
        long[] newValueIsValid = isNull(position) ? new long[] {0} : null;
        return new VariantBlock(0, 1, newValueIsValid, newMetadata, newValues, shreddingSchema, newTypedValue);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkValidPosition(position, positionCount);

        if (isNull(position)) {
            return 0;
        }

        long size = metadata.getEstimatedDataSizeForStats(startOffset + position) +
                values.getEstimatedDataSizeForStats(startOffset + position);
        if (typedValue != null) {
            size += typedValue.getEstimatedDataSizeForStats(startOffset + position);
        }
        return size;
    }

    @Override
    public boolean isNull(int position)
    {
        if (!mayHaveNull()) {
            return false;
        }
        checkValidPosition(position, positionCount);
        return !Bitmap.isSet(valueIsValid, startOffset, position);
    }

    public record VariantNestedBlocks(Block metadataBlock, Block valueBlock)
    {
        public VariantNestedBlocks
        {
            requireNonNull(metadataBlock, "metadataBlock is null");
            requireNonNull(valueBlock, "valueBlock is null");
        }
    }

    /**
     * Returns the nested variant fields from the specified block. The block maybe a RunLengthEncodedBlock, or
     * DictionaryBlock, but the underlying block must be a VariantBlock. The returned nested blocks will be the same
     * length as the specified block, which means they are not null suppressed.
     */
    // this code was copied from RowBlock
    public static VariantNestedBlocks getNestedFields(Block block)
    {
        block = unshred(block);
        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            VariantBlock variantBlock = (VariantBlock) runLengthEncodedBlock.getValue();
            return new VariantNestedBlocks(
                    RunLengthEncodedBlock.create(variantBlock.getMetadata(), runLengthEncodedBlock.getPositionCount()),
                    RunLengthEncodedBlock.create(variantBlock.getValues(), runLengthEncodedBlock.getPositionCount()));
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            VariantBlock variantBlock = (VariantBlock) dictionaryBlock.getDictionary();
            return new VariantNestedBlocks(
                    dictionaryBlock.createProjection(variantBlock.getMetadata()),
                    dictionaryBlock.createProjection(variantBlock.getValues()));
        }
        if (block instanceof VariantBlock variantBlock) {
            return new VariantNestedBlocks(variantBlock.getMetadata(), variantBlock.getValues());
        }
        throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
    }

    /**
     * Returns the nested variant fields from the specified block with null variants suppressed. The block maybe a RunLengthEncodedBlock, or
     * DictionaryBlock, but the underlying block must be a VariantBlock. The returned nested blocks will not be the same
     * length as the specified block if it contains null variants.
     */
    // this code was copied from RowBlock
    public static VariantNestedBlocks getNullSuppressedNestedFields(Block block)
    {
        block = unshred(block);
        if (!block.mayHaveNull()) {
            return getNestedFields(block);
        }

        return switch (block) {
            case RunLengthEncodedBlock runLengthEncodedBlock -> {
                VariantBlock variantBlock = (VariantBlock) runLengthEncodedBlock.getValue();
                if (!variantBlock.isNull(0)) {
                    throw new IllegalStateException("Expected run length encoded block value to be null");
                }
                // all values are null, so return a zero-length block of the correct type
                yield new VariantNestedBlocks(
                        variantBlock.getMetadata().getRegion(0, 0),
                        variantBlock.getValues().getRegion(0, 0));
            }
            case DictionaryBlock dictionaryBlock -> {
                int[] newIds = new int[dictionaryBlock.getPositionCount()];
                int idCount = 0;
                for (int position = 0; position < newIds.length; position++) {
                    if (!dictionaryBlock.isNull(position)) {
                        newIds[idCount] = dictionaryBlock.getId(position);
                        idCount++;
                    }
                }
                int nonNullPositionCount = idCount;
                VariantBlock variantBlock = (VariantBlock) dictionaryBlock.getDictionary();
                yield new VariantNestedBlocks(
                        DictionaryBlock.create(nonNullPositionCount, variantBlock.getMetadata(), newIds),
                        DictionaryBlock.create(nonNullPositionCount, variantBlock.getValues(), newIds));
            }
            case VariantBlock variantBlock -> {
                int[] nonNullPositions = new int[variantBlock.getPositionCount()];
                int idCount = 0;
                for (int position = 0; position < nonNullPositions.length; position++) {
                    if (!variantBlock.isNull(position)) {
                        nonNullPositions[idCount] = position;
                        idCount++;
                    }
                }
                int nonNullPositionCount = idCount;
                yield new VariantNestedBlocks(
                        DictionaryBlock.create(nonNullPositionCount, variantBlock.getMetadata(), nonNullPositions),
                        DictionaryBlock.create(nonNullPositionCount, variantBlock.getValues(), nonNullPositions));
            }
            default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
        };
    }

    /// Returns an equivalent block in which the underlying variant block is not shredded.
    /// The block may be a RunLengthEncodedBlock or DictionaryBlock over a VariantBlock.
    public static Block unshred(Block block)
    {
        return switch (block) {
            case RunLengthEncodedBlock rle when ((VariantBlock) rle.getValue()).isShredded() -> RunLengthEncodedBlock.create(((VariantBlock) rle.getValue()).unshred(), rle.getPositionCount());
            case DictionaryBlock dictionary when ((VariantBlock) dictionary.getDictionary()).isShredded() -> dictionary.createProjection(((VariantBlock) dictionary.getDictionary()).unshred());
            case VariantBlock variantBlock -> variantBlock.unshred();
            default -> block;
        };
    }

    @Override
    public VariantBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public Optional<Bitmap> getValidityBitmap()
    {
        if (valueIsValid == null) {
            return Optional.empty();
        }
        return Optional.of(new Bitmap(valueIsValid, startOffset, positionCount));
    }
}

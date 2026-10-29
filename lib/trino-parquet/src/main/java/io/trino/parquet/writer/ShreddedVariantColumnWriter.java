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
import io.trino.parquet.writer.repdef.DefLevelWriterProvider;
import io.trino.parquet.writer.repdef.DefLevelWriterProviders;
import io.trino.parquet.writer.repdef.RepLevelWriterProvider;
import io.trino.parquet.writer.repdef.RepLevelWriterProviders;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.variant.VariantShredder;
import io.trino.spi.variant.VariantShreddingSchema;

import java.io.IOException;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/// Writes a VARIANT column using the shredded layout described by the shredding schema.
/// Values are shredded as they are written, unless the input is already shredded with the same schema.
public class ShreddedVariantColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = instanceSize(ShreddedVariantColumnWriter.class);

    private final ColumnWriter metadataColumnWriter;
    private final ColumnWriter valueColumnWriter;
    private final ColumnWriter typedValueColumnWriter;
    private final VariantShreddingSchema schema;
    private final int maxDefinitionLevel;

    public ShreddedVariantColumnWriter(
            ColumnWriter metadataColumnWriter,
            ColumnWriter valueColumnWriter,
            ColumnWriter typedValueColumnWriter,
            VariantShreddingSchema schema,
            int maxDefinitionLevel)
    {
        this.metadataColumnWriter = requireNonNull(metadataColumnWriter, "metadataColumnWriter is null");
        this.valueColumnWriter = requireNonNull(valueColumnWriter, "valueColumnWriter is null");
        this.typedValueColumnWriter = requireNonNull(typedValueColumnWriter, "typedValueColumnWriter is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.maxDefinitionLevel = maxDefinitionLevel;
    }

    @Override
    public void writeBlock(ColumnChunk columnChunk)
            throws IOException
    {
        VariantBlock block = VariantShredder.shred(columnChunk.getBlock(), schema);

        Block metadata = block.getMetadata();
        Block residual = block.getResidualValues();
        Block typedValue = block.getTypedValue();
        if (block.mayHaveNull()) {
            // like the struct writer, the nested values of null variants are not written
            int[] nonNullPositions = new int[block.getPositionCount()];
            int nonNullCount = 0;
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    nonNullPositions[nonNullCount] = position;
                    nonNullCount++;
                }
            }
            metadata = DictionaryBlock.create(nonNullCount, metadata, nonNullPositions);
            residual = DictionaryBlock.create(nonNullCount, residual, nonNullPositions);
            typedValue = DictionaryBlock.create(nonNullCount, typedValue, nonNullPositions);
        }

        List<DefLevelWriterProvider> defLevelWriterProviders = ImmutableList.<DefLevelWriterProvider>builder()
                .addAll(columnChunk.getDefLevelWriterProviders())
                .add(DefLevelWriterProviders.of(block, maxDefinitionLevel))
                .build();
        List<RepLevelWriterProvider> repLevelWriterProviders = ImmutableList.<RepLevelWriterProvider>builder()
                .addAll(columnChunk.getRepLevelWriterProviders())
                .add(RepLevelWriterProviders.of(block))
                .build();

        metadataColumnWriter.writeBlock(new ColumnChunk(metadata, defLevelWriterProviders, repLevelWriterProviders));
        valueColumnWriter.writeBlock(new ColumnChunk(residual, defLevelWriterProviders, repLevelWriterProviders));
        typedValueColumnWriter.writeBlock(new ColumnChunk(typedValue, defLevelWriterProviders, repLevelWriterProviders));
    }

    @Override
    public void close()
    {
        metadataColumnWriter.close();
        valueColumnWriter.close();
        typedValueColumnWriter.close();
    }

    @Override
    public List<BufferData> getBuffer()
            throws IOException
    {
        return ImmutableList.<BufferData>builder()
                .addAll(metadataColumnWriter.getBuffer())
                .addAll(valueColumnWriter.getBuffer())
                .addAll(typedValueColumnWriter.getBuffer())
                .build();
    }

    @Override
    public long getEstimatedBufferedBytes(CompressionStats compressionStats)
    {
        return metadataColumnWriter.getEstimatedBufferedBytes(compressionStats) +
                valueColumnWriter.getEstimatedBufferedBytes(compressionStats) +
                typedValueColumnWriter.getEstimatedBufferedBytes(compressionStats);
    }

    @Override
    public CompressionStats getCompressionStats()
    {
        return metadataColumnWriter.getCompressionStats()
                .add(valueColumnWriter.getCompressionStats())
                .add(typedValueColumnWriter.getCompressionStats());
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + metadataColumnWriter.getRetainedBytes() + valueColumnWriter.getRetainedBytes() + typedValueColumnWriter.getRetainedBytes();
    }
}

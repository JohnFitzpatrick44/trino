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

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.VariantFields;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.parquet.writer.VariantShreddingInference;
import io.trino.plugin.hive.RollbackAction;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.variant.VariantShreddingSchema;
import org.apache.iceberg.MetricsConfig;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static io.trino.plugin.iceberg.util.ParquetUtil.footerMetrics;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class IcebergParquetFileWriter
        implements IcebergFileWriter
{
    private static final int INSTANCE_SIZE = instanceSize(IcebergParquetFileWriter.class);

    private final MetricsConfig metricsConfig;
    private final Location location;
    private final WriterFactory writerFactory;
    private final OptionalInt variantInferenceRows;
    private final List<Page> bufferedPages = new ArrayList<>();
    private long bufferedRows;
    private long bufferedBytes;
    private ParquetFileWriter parquetFileWriter;

    public IcebergParquetFileWriter(
            MetricsConfig metricsConfig,
            TrinoOutputFile outputFile,
            RollbackAction rollbackAction,
            List<Type> fileColumnTypes,
            List<String> fileColumnNames,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions parquetWriterOptions,
            int[] fileInputColumnIndexes,
            CompressionCodec compressionCodec,
            String trinoVersion)
            throws IOException
    {
        this(metricsConfig, outputFile, rollbackAction, fileColumnTypes, fileColumnNames, messageType, primitiveTypes, parquetWriterOptions, fileInputColumnIndexes, compressionCodec, trinoVersion, OptionalInt.empty());
    }

    /// Creates a writer that shreds the VARIANT columns if `variantInferenceRows` is present. The shredding schema of
    /// a column is inferred from the first rows written, and the rows are buffered until the schema is inferred.
    public IcebergParquetFileWriter(
            MetricsConfig metricsConfig,
            TrinoOutputFile outputFile,
            RollbackAction rollbackAction,
            List<Type> fileColumnTypes,
            List<String> fileColumnNames,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions parquetWriterOptions,
            int[] fileInputColumnIndexes,
            CompressionCodec compressionCodec,
            String trinoVersion,
            OptionalInt variantInferenceRows)
            throws IOException
    {
        this.location = outputFile.location();
        this.metricsConfig = requireNonNull(metricsConfig, "metricsConfig is null");
        requireNonNull(variantInferenceRows, "variantInferenceRows is null");
        variantInferenceRows.ifPresent(rows -> checkArgument(rows > 0, "variantInferenceRows must be positive"));
        this.writerFactory = new WriterFactory(outputFile, rollbackAction, fileColumnTypes, fileColumnNames, messageType, primitiveTypes, parquetWriterOptions, fileInputColumnIndexes, compressionCodec, trinoVersion);
        if (variantInferenceRows.isPresent() && fileColumnTypes.contains(VARIANT)) {
            this.variantInferenceRows = variantInferenceRows;
        }
        else {
            this.variantInferenceRows = OptionalInt.empty();
            this.parquetFileWriter = writerFactory.create(messageType);
        }
    }

    @Override
    public FileMetrics getFileMetrics()
    {
        ParquetMetadata parquetMetadata;
        try {
            parquetMetadata = new ParquetMetadata(parquetFileWriter.getFileMetadata(), new ParquetDataSourceId(location.toString()), Optional.empty());
            return new FileMetrics(footerMetrics(parquetMetadata, Stream.empty(), metricsConfig), Optional.of(getSplitOffsets(parquetMetadata)));
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("Error creating metadata for Parquet file %s", location), e);
        }
    }

    @Override
    public long getWrittenBytes()
    {
        if (parquetFileWriter == null) {
            return 0;
        }
        return parquetFileWriter.getWrittenBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        if (parquetFileWriter == null) {
            return INSTANCE_SIZE + bufferedBytes;
        }
        return INSTANCE_SIZE + parquetFileWriter.getMemoryUsage();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        if (parquetFileWriter != null) {
            parquetFileWriter.appendRows(dataPage);
            return;
        }
        bufferedPages.add(dataPage);
        bufferedRows += dataPage.getPositionCount();
        bufferedBytes += dataPage.getRetainedSizeInBytes();
        if (bufferedRows >= variantInferenceRows.orElseThrow()) {
            startWriting();
        }
    }

    @Override
    public RollbackAction commit()
    {
        if (parquetFileWriter == null) {
            startWriting();
        }
        return parquetFileWriter.commit();
    }

    @Override
    public void rollback()
    {
        if (parquetFileWriter != null) {
            parquetFileWriter.rollback();
        }
        bufferedPages.clear();
    }

    @Override
    public long getValidationCpuNanos()
    {
        if (parquetFileWriter == null) {
            return 0;
        }
        return parquetFileWriter.getValidationCpuNanos();
    }

    /// Infers the shredding schemas of the VARIANT columns from the buffered rows, and writes the buffered rows
    private void startWriting()
    {
        MessageType messageType = writerFactory.messageType();
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>(messageType.getFields());
        for (int column = 0; column < fields.size(); column++) {
            org.apache.parquet.schema.Type field = fields.get(column);
            if (writerFactory.fileColumnTypes().get(column) != VARIANT || field.isPrimitive()) {
                continue;
            }
            int channel = writerFactory.fileInputColumnIndex(column);
            List<Block> blocks = bufferedPages.stream()
                    .map(page -> page.getBlock(channel))
                    .collect(toImmutableList());
            Optional<VariantShreddingSchema> schema = VariantShreddingInference.inferShreddingSchema(blocks);
            if (schema.isPresent()) {
                org.apache.parquet.schema.Type shreddedField = VariantFields.toParquetType(field.getName(), field.getRepetition(), schema);
                if (field.getId() != null) {
                    shreddedField = shreddedField.withId(field.getId().intValue());
                }
                fields.set(column, shreddedField);
            }
        }
        try {
            parquetFileWriter = writerFactory.create(new MessageType(messageType.getName(), fields));
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
        for (Page page : bufferedPages) {
            parquetFileWriter.appendRows(page);
        }
        bufferedPages.clear();
        bufferedRows = 0;
        bufferedBytes = 0;
    }

    private static List<Long> getSplitOffsets(ParquetMetadata metadata)
            throws IOException
    {
        List<BlockMetadata> blocks = metadata.getBlocks();
        List<Long> splitOffsets = new ArrayList<>(blocks.size());
        for (BlockMetadata blockMetaData : blocks) {
            splitOffsets.add(blockMetaData.columns().getFirst().getStartingPos());
        }
        Collections.sort(splitOffsets);
        return ImmutableList.copyOf(splitOffsets);
    }

    private static final class WriterFactory
    {
        private final TrinoOutputFile outputFile;
        private final RollbackAction rollbackAction;
        private final List<Type> fileColumnTypes;
        private final List<String> fileColumnNames;
        private final MessageType messageType;
        private final Map<List<String>, Type> primitiveTypes;
        private final ParquetWriterOptions parquetWriterOptions;
        private final int[] fileInputColumnIndexes;
        private final CompressionCodec compressionCodec;
        private final String trinoVersion;

        private WriterFactory(
                TrinoOutputFile outputFile,
                RollbackAction rollbackAction,
                List<Type> fileColumnTypes,
                List<String> fileColumnNames,
                MessageType messageType,
                Map<List<String>, Type> primitiveTypes,
                ParquetWriterOptions parquetWriterOptions,
                int[] fileInputColumnIndexes,
                CompressionCodec compressionCodec,
                String trinoVersion)
        {
            this.outputFile = requireNonNull(outputFile, "outputFile is null");
            this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");
            this.fileColumnTypes = ImmutableList.copyOf(fileColumnTypes);
            this.fileColumnNames = ImmutableList.copyOf(fileColumnNames);
            this.messageType = requireNonNull(messageType, "messageType is null");
            this.primitiveTypes = requireNonNull(primitiveTypes, "primitiveTypes is null");
            this.parquetWriterOptions = requireNonNull(parquetWriterOptions, "parquetWriterOptions is null");
            this.fileInputColumnIndexes = fileInputColumnIndexes.clone();
            this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
            this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        }

        public MessageType messageType()
        {
            return messageType;
        }

        public List<Type> fileColumnTypes()
        {
            return fileColumnTypes;
        }

        public int fileInputColumnIndex(int column)
        {
            return fileInputColumnIndexes[column];
        }

        public ParquetFileWriter create(MessageType messageType)
                throws IOException
        {
            return new ParquetFileWriter(
                    outputFile,
                    rollbackAction,
                    fileColumnTypes,
                    fileColumnNames,
                    messageType,
                    primitiveTypes,
                    parquetWriterOptions,
                    fileInputColumnIndexes,
                    compressionCodec,
                    trinoVersion,
                    Optional.empty(),
                    Optional.empty());
        }
    }
}

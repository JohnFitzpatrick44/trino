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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;

/// Tables written by Spark or other Iceberg writers can contain shredded VARIANT columns, where
/// parts of the values are stored in typed Parquet columns. Reading them is not supported yet.
final class TestIcebergUnsupportedShreddedVariant
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", "3")
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                .build();
    }

    @Test
    void testReadShreddedVariant()
            throws IOException
    {
        try (TestTable table = newTrinoTable("test_shredded_variant", "(id integer, v variant)")) {
            appendShreddedFile(table.getName());

            assertQueryFails("SELECT v FROM " + table.getName(), "Reading shredded VARIANT columns is not supported: v");
            assertQueryFails("SELECT CAST(v['k'] AS varchar) FROM " + table.getName(), "Reading shredded VARIANT columns is not supported: v");

            // the other columns can be read
            assertQuery("SELECT id FROM " + table.getName(), "VALUES 1, 2");

            // the statistics do not use the variant bounds written by Iceberg for the shredded fields
            assertThat(computeActual("SHOW STATS FOR " + table.getName()).getRowCount()).isEqualTo(3);
            assertThat(computeActual("EXPLAIN SELECT v FROM " + table.getName()).getOnlyValue()).isNotNull();
        }
    }

    private void appendShreddedFile(String tableName)
            throws IOException
    {
        BaseTable table = IcebergTestUtils.loadTable(tableName, getHiveMetastore(getQueryRunner()), getFileSystemFactory(getQueryRunner()), ICEBERG_CATALOG, "tpch");
        // the shredded type of the variant: an object with a string field k
        Type typedValue = Types.optionalGroup()
                .addField(Types.requiredGroup()
                        .optional(BINARY).named("value")
                        .optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("typed_value")
                        .named("k"))
                .named("typed_value");
        String dataPath = table.location() + "/data/shredded-" + UUID.randomUUID() + ".parquet";
        try (DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(dataPath))
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .createWriterFunc(GenericParquetWriter::create)
                .variantShreddingFunc((_, _) -> typedValue)
                .build()) {
            for (int id = 1; id <= 2; id++) {
                VariantMetadata metadata = Variants.metadata("k");
                ShreddedObject object = Variants.object(metadata);
                object.put("k", Variants.of("value" + id));
                Record record = GenericRecord.create(table.schema());
                record.setField("id", id);
                record.setField("v", Variant.of(metadata, object));
                writer.write(record);
            }
            writer.close();
            table.newFastAppend()
                    .appendFile(writer.toDataFile())
                    .commit();
        }
    }
}

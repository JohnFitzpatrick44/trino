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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.metastore.HiveMetastore;
import io.trino.parquet.VariantFields;
import io.trino.spi.QueryId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.variant.VariantShreddingSchema;
import io.trino.spi.variant.VariantShreddingSchema.ArraySchema;
import io.trino.spi.variant.VariantShreddingSchema.Field;
import io.trino.spi.variant.VariantShreddingSchema.ObjectSchema;
import io.trino.spi.variant.VariantShreddingSchema.Primitive;
import io.trino.spi.variant.VariantShreddingSchema.ValueGroup;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.schema.GroupType;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.COMPLEX_EXPRESSION_PUSHDOWN;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergShreddedVariant
        extends AbstractTestQueryFramework
{
    private static final List<String> VALUES = ImmutableList.of(
            "{\"id\": 1, \"name\": \"alice\", \"tags\": [\"a\", \"b\"], \"location\": {\"lat\": 1.5, \"lon\": -2.5}}",
            "{\"id\": 2, \"name\": \"bob\", \"extra\": true, \"tags\": [], \"location\": {\"lat\": 3.5, \"lon\": 4.5, \"alt\": 10}}",
            "{\"id\": \"three\", \"name\": null, \"location\": \"unknown\"}",
            "{\"name\": \"no id\", \"tags\": [\"c\", null, 1]}",
            "{}",
            "\"not an object\"",
            "[1, 2, 3]",
            "null",
            "{\"id\": 6, \"Name\": \"case sensitive\", \"user.name\": \"dotted\"}");

    private static final ObjectSchema SCHEMA = new ObjectSchema(ImmutableList.of(
            new Field(utf8Slice("Name"), ValueGroup.typed(new Primitive(VARCHAR))),
            new Field(utf8Slice("id"), ValueGroup.typed(new Primitive(INTEGER))),
            new Field(utf8Slice("location"), ValueGroup.typed(new ObjectSchema(ImmutableList.of(
                    new Field(utf8Slice("lat"), ValueGroup.typed(new Primitive(DOUBLE))),
                    new Field(utf8Slice("lon"), ValueGroup.typed(new Primitive(DOUBLE))))))),
            new Field(utf8Slice("name"), ValueGroup.typed(new Primitive(VARCHAR))),
            new Field(utf8Slice("tags"), ValueGroup.typed(new ArraySchema(ValueGroup.typed(new Primitive(VARCHAR))))),
            new Field(utf8Slice("user.name"), ValueGroup.typed(new Primitive(VARCHAR)))));

    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", "3")
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                .build();
    }

    @BeforeAll
    void setUp()
    {
        metastore = getHiveMetastore(getQueryRunner());
    }

    @Test
    void testRead()
            throws IOException
    {
        try (TestTable shredded = shreddedTable("test_shredded_read", SCHEMA);
                TestTable expected = unshreddedTable("test_unshredded_read")) {
            assertSameResults(shredded, expected, "SELECT id, v FROM %s");
            assertSameResults(shredded, expected, "SELECT id, CAST(v AS JSON) FROM %s");
            assertSameResults(shredded, expected, "SELECT id, variant_is_null(v) FROM %s");
            assertSameResults(shredded, expected, "SELECT id, CAST(v['tags'] AS JSON) FROM %s WHERE id < 5");
            assertSameResults(shredded, expected, "SELECT count(*), count(v) FROM %s");
        }
    }

    @Test
    void testSubscripts()
            throws IOException
    {
        try (TestTable shredded = shreddedTable("test_shredded_subscripts", SCHEMA);
                TestTable expected = unshreddedTable("test_unshredded_subscripts")) {
            // only the objects are accessed, because a subscript fails on other values
            String objects = " WHERE id < 6 OR id = 9";
            assertSameResults(shredded, expected, "SELECT id, v['name'], CAST(v['name'] AS varchar) FROM %s" + objects);
            assertSameResults(shredded, expected, "SELECT id, v['Name'], v['user.name'], v['missing'] FROM %s" + objects);
            assertSameResults(shredded, expected, "SELECT id, v['location'], CAST(v['location'] AS JSON) FROM %s" + objects);
            assertSameResults(shredded, expected, "SELECT id, CAST(v['tags'] AS JSON) FROM %s" + objects);
            assertSameResults(shredded, expected, "SELECT id, v['tags'][1] FROM %s WHERE id IN (1, 4)");
            assertSameResults(shredded, expected, "SELECT id, CAST(v['location']['lat'] AS double) FROM %s WHERE id IN (1, 2)");
            assertSameResults(shredded, expected, "SELECT id, v['id'] FROM %s" + objects);
            assertSameResults(shredded, expected, "SELECT id FROM %s WHERE CASE WHEN id < 6 THEN CAST(v['name'] AS varchar) END = 'bob'");
            assertSameResults(shredded, expected, "SELECT id, CAST(v['name'] AS varchar) FROM %s WHERE id IN (1, 2, 3) AND CASE WHEN id < 6 THEN CAST(v['name'] AS varchar) END LIKE '%%o%%'");
            assertSameResults(shredded, expected, "SELECT v[2] FROM %s WHERE id = 7");

            // the subscripts fail the same way
            assertQueryFails("SELECT v['name'] FROM " + shredded.getName() + " WHERE id = 6", "VARIANT value is string, not an object");
            assertQueryFails("SELECT v['name'] FROM " + shredded.getName() + " WHERE id = 7", "VARIANT value is array, not an object");
            assertQueryFails("SELECT CAST(v['id'] AS bigint) FROM " + shredded.getName() + " WHERE id = 3", ".*Cannot cast 'three' to BIGINT.*");
        }
    }

    @Test
    void testProjectionPushdown()
            throws IOException
    {
        try (TestTable shredded = shreddedTable("test_shredded_projection", SCHEMA)) {
            @Language("SQL") String query = "SELECT CAST(v['location']['lat'] AS double) FROM " + shredded.getName() + " WHERE id IN (1, 2)";
            assertThat(query(query)).matches("VALUES DOUBLE '1.5', DOUBLE '3.5'");
            // the scan reads a pruned variant, and the planner does not evaluate the prune
            String plan = (String) computeActual("EXPLAIN " + query).getOnlyValue();
            assertThat(plan)
                    .contains("v#$['location']['lat']")
                    .doesNotContain("$variant_prune");

            // the results are the same without pushdown
            Session withoutPushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(ICEBERG_CATALOG, "projection_pushdown_enabled", "false")
                    .build();
            assertThat(query(withoutPushdown, query)).matches("VALUES DOUBLE '1.5', DOUBLE '3.5'");
            Session withoutComplexExpressions = Session.builder(getSession())
                    .setSystemProperty(COMPLEX_EXPRESSION_PUSHDOWN, "false")
                    .build();
            assertThat(query(withoutComplexExpressions, query)).matches("VALUES DOUBLE '1.5', DOUBLE '3.5'");
            assertThat((String) computeActual(withoutComplexExpressions, "EXPLAIN " + query).getOnlyValue())
                    .doesNotContain("$variant_prune")
                    .doesNotContain("v#$");
        }
    }

    @Test
    void testProjectionPushdownReadsLessData()
            throws IOException
    {
        // the payload is large, so its columns are not merged with the reads of the other columns
        ObjectSchema schema = new ObjectSchema(ImmutableList.of(
                new Field(utf8Slice("name"), ValueGroup.typed(new Primitive(VARCHAR))),
                new Field(utf8Slice("payload"), ValueGroup.typed(new Primitive(VARCHAR)))));
        Random random = new Random(42);
        ImmutableList.Builder<io.trino.spi.variant.Variant> variants = ImmutableList.builder();
        for (int i = 0; i < 20_000; i++) {
            byte[] payload = new byte[300];
            random.nextBytes(payload);
            variants.add(io.trino.spi.variant.Variant.ofObject(ImmutableMap.of(
                    utf8Slice("name"), io.trino.spi.variant.Variant.ofString("name" + i),
                    utf8Slice("payload"), io.trino.spi.variant.Variant.ofString(Base64.getEncoder().encodeToString(payload)))));
        }
        try (TestTable table = newTrinoTable("test_shredded_projection_bytes", "(id integer, v variant)")) {
            appendShreddedFile(table.getName(), schema, variants.build(), ImmutableMap.of());
            long prunedBytes = physicalInputBytes(getSession(), "SELECT CAST(v['name'] AS varchar) FROM " + table.getName() + " WHERE id IS NOT NULL");
            long fullBytes = physicalInputBytes(getSession(), "SELECT v FROM " + table.getName() + " WHERE id IS NOT NULL");
            assertThat(prunedBytes).isLessThan(fullBytes / 10);
            assertThat(query("SELECT count(DISTINCT CAST(v['name'] AS varchar)) FROM " + table.getName() + " WHERE id IS NOT NULL"))
                    .matches("VALUES BIGINT '20000'");
        }
    }

    @Test
    void testPredicatePushdownSkipsFiles()
            throws IOException
    {
        ObjectSchema schema = new ObjectSchema(ImmutableList.of(
                new Field(utf8Slice("id"), ValueGroup.typed(new Primitive(INTEGER))),
                new Field(utf8Slice("name"), ValueGroup.typed(new Primitive(VARCHAR)))));
        try (TestTable table = newTrinoTable("test_shredded_predicate_files", "(id integer, v variant)")) {
            // three files with disjoint ranges of ids
            for (int file = 0; file < 3; file++) {
                appendShreddedFile(table.getName(), schema, objects(file * 1000, 1000), ImmutableMap.of());
            }
            // a row with a null variant is added to each file
            long totalRows = 3 * 1001;
            assertThat(physicalInputPositions(getSession(), "SELECT count(*) FROM " + table.getName() + " WHERE v IS NOT NULL")).isEqualTo(totalRows);

            assertPredicate(table, "CAST(v['id'] AS integer) = 1500", 1, 1001);
            assertPredicate(table, "CAST(v['id'] AS bigint) = 1500", 1, 1001);
            assertPredicate(table, "CAST(v['id'] AS integer) BETWEEN 2100 AND 2200", 101, 1001);
            assertPredicate(table, "CAST(v['id'] AS integer) IN (5, 2500)", 2, 2002);
            assertPredicate(table, "CAST(v['id'] AS integer) > 1999", 1000, 1001);
            assertPredicate(table, "1000 > CAST(v['id'] AS integer)", 1000, 1001);
            assertPredicate(table, "CAST(v['name'] AS varchar) = 'name01500'", 1, 1001);
            assertPredicate(table, "CAST(v['id'] AS integer) = 5000", 0, 0);

            // the files are skipped using the bounds of the shredded fields written by Iceberg
            BaseTable icebergTable = IcebergTestUtils.loadTable(table.getName(), metastore, getFileSystemFactory(getQueryRunner()), ICEBERG_CATALOG, "tpch");
            int variantId = icebergTable.schema().findField("v").fieldId();
            IcebergColumnHandle idExtract = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(variantId, "v"))
                    .fieldType(io.trino.spi.type.VariantType.VARIANT, INTEGER)
                    .variantExtract(ImmutableList.of("id"))
                    .build();
            List<Domain> fileDomains = new ArrayList<>();
            try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().includeColumnStats().planFiles()) {
                for (FileScanTask task : tasks) {
                    fileDomains.add(VariantBounds.variantExtractDomain(idExtract, task.file().lowerBounds().get(variantId), task.file().upperBounds().get(variantId)));
                }
            }
            assertThat(fileDomains).containsExactlyInAnyOrder(
                    Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 0L, true, 999L, true)), true),
                    Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1000L, true, 1999L, true)), true),
                    Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 2000L, true, 2999L, true)), true));
        }
    }

    @Test
    void testPredicatePushdownSkipsRowGroups()
            throws IOException
    {
        ObjectSchema schema = new ObjectSchema(ImmutableList.of(
                new Field(utf8Slice("id"), ValueGroup.typed(new Primitive(INTEGER))),
                new Field(utf8Slice("name"), ValueGroup.typed(new Primitive(VARCHAR)))));
        try (TestTable table = newTrinoTable("test_shredded_predicate_row_groups", "(id integer, v variant)")) {
            appendShreddedFile(table.getName(), schema, objects(0, 10_000), ImmutableMap.of(
                    "write.parquet.row-group-size-bytes", "4096",
                    "write.parquet.row-group-check-min-record-count", "100",
                    "write.parquet.row-group-check-max-record-count", "100"));
            long allRows = physicalInputPositions(getSession(), "SELECT count(*) FROM " + table.getName() + " WHERE v IS NOT NULL");
            assertThat(allRows).isEqualTo(10_001);
            assertThat(query("SELECT CAST(v['name'] AS varchar) FROM " + table.getName() + " WHERE CAST(v['id'] AS integer) = 7777"))
                    .matches("VALUES CAST('name07777' AS varchar)");
            assertThat(physicalInputPositions(getSession(), "SELECT count(*) FROM " + table.getName() + " WHERE CAST(v['id'] AS integer) = 7777"))
                    .isLessThan(allRows / 10);
        }
    }

    @Test
    void testPredicatePushdownWithMixedTypes()
            throws IOException
    {
        ObjectSchema schema = new ObjectSchema(ImmutableList.of(
                new Field(utf8Slice("id"), ValueGroup.typed(new Primitive(INTEGER)))));
        ImmutableList.Builder<io.trino.spi.variant.Variant> variants = ImmutableList.builder();
        variants.addAll(objects(0, 100));
        // a value that does not have the shredded type is stored in the value column, so the statistics do not describe all values
        variants.add(io.trino.spi.variant.Variant.ofObject(ImmutableMap.of(utf8Slice("id"), io.trino.spi.variant.Variant.ofString("12345"))));
        try (TestTable table = newTrinoTable("test_shredded_predicate_mixed", "(id integer, v variant)")) {
            appendShreddedFile(table.getName(), schema, variants.build(), ImmutableMap.of());
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE CAST(v['id'] AS integer) = 12345"))
                    .matches("VALUES 101");
            assertThat(query("SELECT count(*) FROM " + table.getName() + " WHERE CAST(v['id'] AS varchar) = '12345'"))
                    .matches("VALUES BIGINT '1'");
        }
    }

    private void assertPredicate(TestTable table, @Language("SQL") String predicate, long expectedCount, long expectedPositions)
    {
        @Language("SQL") String query = "SELECT count(*) FROM " + table.getName() + " WHERE " + predicate;
        assertThat(query(query)).matches("VALUES BIGINT '%s'".formatted(expectedCount));
        assertThat(physicalInputPositions(getSession(), query)).isEqualTo(expectedPositions);
    }

    private static List<io.trino.spi.variant.Variant> objects(int start, int count)
    {
        ImmutableList.Builder<io.trino.spi.variant.Variant> variants = ImmutableList.builder();
        for (int i = start; i < start + count; i++) {
            variants.add(io.trino.spi.variant.Variant.ofObject(ImmutableMap.of(
                    utf8Slice("id"), io.trino.spi.variant.Variant.ofInt(i),
                    utf8Slice("name"), io.trino.spi.variant.Variant.ofString("name%05d".formatted(i)))));
        }
        return variants.build();
    }

    private long physicalInputPositions(Session session, @Language("SQL") String query)
    {
        QueryId queryId = getDistributedQueryRunner().executeWithPlan(session, query).queryId();
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats().getPhysicalInputPositions();
    }

    @Test
    void testFilterDoesNotFailForFilteredRows()
            throws IOException
    {
        try (TestTable shredded = shreddedTable("test_shredded_filter_safety", SCHEMA)) {
            // the cast fails for the rows that are not objects or that have a string id, which the filter removes first
            assertThat(query("SELECT CAST(v['id'] AS bigint) FROM " + shredded.getName() + " WHERE id IN (1, 2, 9)"))
                    .matches("VALUES BIGINT '1', BIGINT '2', BIGINT '6'");
        }
    }

    @Test
    void testDifferentSchemasInTable()
            throws IOException
    {
        ObjectSchema otherSchema = new ObjectSchema(ImmutableList.of(
                new Field(utf8Slice("id"), ValueGroup.typed(new Primitive(VARCHAR))),
                new Field(utf8Slice("tags"), ValueGroup.valueOnly())));
        try (TestTable shredded = shreddedTable("test_shredded_schemas", SCHEMA);
                TestTable expected = unshreddedTable("test_unshredded_schemas")) {
            appendShreddedFile(shredded.getName(), otherSchema);
            appendShreddedFile(shredded.getName(), new Primitive(BIGINT));
            // the expected table has the same rows three times
            assertUpdate("INSERT INTO " + expected.getName() + " SELECT id, v FROM " + expected.getName() + " CROSS JOIN UNNEST(ARRAY[1, 2])", 2L * (VALUES.size() + 1));
            assertSameResults(shredded, expected, "SELECT id, v FROM %s");
            assertSameResults(shredded, expected, "SELECT id, CAST(v['name'] AS varchar) FROM %s WHERE id IN (1, 2, 3, 4, 5)");
        }
    }

    @Test
    void testModifyShreddedTable()
            throws IOException
    {
        try (TestTable shredded = shreddedTable("test_shredded_modify", SCHEMA);
                TestTable expected = unshreddedTable("test_unshredded_modify")) {
            for (TestTable table : ImmutableList.of(shredded, expected)) {
                assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 2", 1);
                assertUpdate("UPDATE " + table.getName() + " SET v = CAST(JSON '{\"id\": 100}' AS VARIANT) WHERE id = 1", 1);
                assertUpdate("INSERT INTO " + table.getName() + " SELECT id + 100, v FROM " + table.getName() + " WHERE id IN (3, 4)", 2);
            }
            assertSameResults(shredded, expected, "SELECT id, v FROM %s");
        }
    }

    private void assertSameResults(TestTable actual, TestTable expected, @Language("SQL") String query)
    {
        assertThat(query(query.formatted(actual.getName())))
                .matches(query.formatted(expected.getName()));
    }

    private long physicalInputBytes(Session session, @Language("SQL") String query)
    {
        QueryId queryId = getDistributedQueryRunner().executeWithPlan(session, query).queryId();
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats().getPhysicalInputDataSize().toBytes();
    }

    private TestTable unshreddedTable(String name)
    {
        TestTable table = newTrinoTable(name, "(id integer, v variant)");
        StringBuilder insert = new StringBuilder("INSERT INTO " + table.getName() + " VALUES ");
        for (int i = 0; i < VALUES.size(); i++) {
            if (i > 0) {
                insert.append(", ");
            }
            insert.append("(%s, CAST(JSON '%s' AS VARIANT))".formatted(i + 1, VALUES.get(i).replace("'", "''")));
        }
        insert.append(", (").append(VALUES.size() + 1).append(", NULL)");
        assertUpdate(insert.toString(), VALUES.size() + 1);
        return table;
    }

    private TestTable shreddedTable(String name, VariantShreddingSchema schema)
            throws IOException
    {
        TestTable table = newTrinoTable(name, "(id integer, v variant)");
        appendShreddedFile(table.getName(), schema);
        return table;
    }

    private void appendShreddedFile(String tableName, VariantShreddingSchema schema)
            throws IOException
    {
        // compute the variants with Trino, and write them with Iceberg
        List<io.trino.spi.variant.Variant> variants = VALUES.stream()
                .map(value -> (io.trino.spi.variant.Variant) computeScalar("SELECT CAST(JSON '%s' AS VARIANT)".formatted(value.replace("'", "''"))))
                .toList();
        appendShreddedFile(tableName, schema, variants, ImmutableMap.of());
    }

    /// Writes the values with the Iceberg Parquet writer, which shreds the variants using the schema.
    /// A row with a null variant is added after the values.
    private void appendShreddedFile(String tableName, VariantShreddingSchema schema, List<io.trino.spi.variant.Variant> variants, Map<String, String> properties)
            throws IOException
    {
        BaseTable table = IcebergTestUtils.loadTable(tableName, metastore, getFileSystemFactory(getQueryRunner()), ICEBERG_CATALOG, "tpch");
        GroupType typedValueParent = VariantFields.toParquetType("v", OPTIONAL, Optional.of(schema));
        String dataPath = table.location() + "/data/shredded-" + UUID.randomUUID() + ".parquet";
        try (DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(dataPath))
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .createWriterFunc(GenericParquetWriter::create)
                .setAll(properties)
                .variantShreddingFunc((_, _) -> typedValueParent.getType("typed_value"))
                .build()) {
            for (int i = 0; i < variants.size(); i++) {
                Record record = GenericRecord.create(table.schema());
                record.setField("id", i + 1);
                record.setField("v", toIcebergVariant(variants.get(i)));
                writer.write(record);
            }
            Record nullRecord = GenericRecord.create(table.schema());
            nullRecord.setField("id", variants.size() + 1);
            writer.write(nullRecord);
            writer.close();
            table.newFastAppend()
                    .appendFile(writer.toDataFile())
                    .commit();
        }
    }

    private static Variant toIcebergVariant(io.trino.spi.variant.Variant variant)
    {
        VariantMetadata metadata = VariantMetadata.from(byteBuffer(variant.metadata().toSlice()));
        VariantValue value = VariantValue.from(metadata, byteBuffer(variant.data()));
        return Variant.of(metadata, value);
    }

    private static ByteBuffer byteBuffer(Slice slice)
    {
        return ByteBuffer.wrap(slice.getBytes()).order(LITTLE_ENDIAN);
    }
}

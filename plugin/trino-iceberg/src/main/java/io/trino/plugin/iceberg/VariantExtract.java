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
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.util.Objects.requireNonNull;

/// The value at a field name path of a VARIANT column, cast to a type, like `CAST(v['a']['b'] AS bigint)`.
public record VariantExtract(IcebergColumnHandle column, List<String> path, Type type)
{
    public VariantExtract
    {
        requireNonNull(column, "column is null");
        path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
        requireNonNull(type, "type is null");
        checkArgument(column.isBaseColumn() && !column.isPrunedVariant() && column.getType().equals(VARIANT), "Not a VARIANT column: %s", column);
        checkArgument(!path.isEmpty(), "path is empty");
    }
}

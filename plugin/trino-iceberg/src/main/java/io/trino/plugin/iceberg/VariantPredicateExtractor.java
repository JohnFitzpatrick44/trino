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

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressions.extractConjuncts;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.BETWEEN_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;

/// Extracts predicates on values in VARIANT columns, like `CAST(v['a']['b'] AS bigint) > 5`, as domains on
/// [VariantExtract]s. The domains are only used to skip data, and the predicates
/// are still evaluated by the engine. Skipping data never adds failures, but may skip rows where the subscript
/// or cast would fail.
///
/// The domains never allow nulls, because the extracted value can be null even if the variant is not.
final class VariantPredicateExtractor
{
    private static final FunctionName SUBSCRIPT_FUNCTION_NAME = new FunctionName("$operator$subscript");

    private VariantPredicateExtractor() {}

    public static TupleDomain<VariantExtract> extractVariantPredicates(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        Map<VariantExtract, Domain> domains = new HashMap<>();
        for (ConnectorExpression conjunct : extractConjuncts(expression)) {
            toDomain(conjunct, assignments).ifPresent(extractDomain -> domains.merge(extractDomain.extract(), extractDomain.domain(), Domain::intersect));
        }
        return TupleDomain.withColumnDomains(domains);
    }

    private static Optional<ExtractDomain> toDomain(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        if (!(expression instanceof Call call)) {
            return Optional.empty();
        }
        List<ConnectorExpression> arguments = call.getArguments();
        FunctionName name = call.getFunctionName();
        if (name.equals(BETWEEN_FUNCTION_NAME) && arguments.size() == 3) {
            return variantExtract(arguments.getFirst(), assignments).flatMap(extract -> {
                Optional<Object> low = constantValue(arguments.get(1), extract.type());
                Optional<Object> high = constantValue(arguments.get(2), extract.type());
                if (low.isEmpty() || high.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(new ExtractDomain(extract, Domain.create(ValueSet.ofRanges(Range.range(extract.type(), low.get(), true, high.get(), true)), false)));
            });
        }
        if (name.equals(IN_PREDICATE_FUNCTION_NAME) && arguments.size() == 2 &&
                arguments.get(1) instanceof Call array && array.getFunctionName().equals(ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
            return variantExtract(arguments.getFirst(), assignments).flatMap(extract -> {
                List<Object> values = new ArrayList<>();
                for (ConnectorExpression element : array.getArguments()) {
                    Optional<Object> value = constantValue(element, extract.type());
                    if (value.isEmpty()) {
                        return Optional.empty();
                    }
                    values.add(value.get());
                }
                return Optional.of(new ExtractDomain(extract, Domain.multipleValues(extract.type(), values)));
            });
        }
        if (arguments.size() != 2) {
            return Optional.empty();
        }

        Optional<VariantExtract> left = variantExtract(arguments.get(0), assignments);
        if (left.isPresent()) {
            return constantValue(arguments.get(1), left.get().type())
                    .flatMap(value -> comparisonDomain(name, left.get().type(), value))
                    .map(domain -> new ExtractDomain(left.get(), domain));
        }
        Optional<VariantExtract> right = variantExtract(arguments.get(1), assignments);
        if (right.isPresent()) {
            return constantValue(arguments.get(0), right.get().type())
                    .flatMap(value -> comparisonDomain(flip(name), right.get().type(), value))
                    .map(domain -> new ExtractDomain(right.get(), domain));
        }
        return Optional.empty();
    }

    private static Optional<Domain> comparisonDomain(FunctionName name, Type type, Object value)
    {
        Range range;
        if (name.equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            range = Range.equal(type, value);
        }
        else if (name.equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            range = Range.lessThan(type, value);
        }
        else if (name.equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            range = Range.lessThanOrEqual(type, value);
        }
        else if (name.equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            range = Range.greaterThan(type, value);
        }
        else if (name.equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            range = Range.greaterThanOrEqual(type, value);
        }
        else {
            return Optional.empty();
        }
        return Optional.of(Domain.create(ValueSet.ofRanges(range), false));
    }

    private static FunctionName flip(FunctionName name)
    {
        if (name.equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            return GREATER_THAN_OPERATOR_FUNCTION_NAME;
        }
        if (name.equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        }
        if (name.equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return LESS_THAN_OPERATOR_FUNCTION_NAME;
        }
        if (name.equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        }
        return name;
    }

    private static Optional<Object> constantValue(ConnectorExpression expression, Type type)
    {
        if (expression instanceof Constant constant && constant.getValue() != null && constant.getType().equals(type)) {
            return Optional.of(constant.getValue());
        }
        return Optional.empty();
    }

    /// Matches `CAST(v[name]...[name] AS type)` where `v` is a VARIANT column
    private static Optional<VariantExtract> variantExtract(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        if (!(expression instanceof Call cast) || !cast.getFunctionName().equals(CAST_FUNCTION_NAME) || cast.getArguments().size() != 1 || !isSupportedType(cast.getType())) {
            return Optional.empty();
        }
        List<String> path = new ArrayList<>();
        ConnectorExpression current = cast.getArguments().getFirst();
        while (current instanceof Call subscript &&
                subscript.getFunctionName().equals(SUBSCRIPT_FUNCTION_NAME) &&
                subscript.getArguments().size() == 2 &&
                subscript.getArguments().getFirst().getType().equals(VARIANT)) {
            if (!(subscript.getArguments().get(1) instanceof Constant key) || !(key.getValue() instanceof Slice name)) {
                return Optional.empty();
            }
            path.addFirst(name.toStringUtf8());
            current = subscript.getArguments().getFirst();
        }
        if (path.isEmpty() ||
                !(current instanceof Variable variable) ||
                !(assignments.get(variable.getName()) instanceof IcebergColumnHandle column) ||
                !column.isBaseColumn() ||
                !column.getType().equals(VARIANT)) {
            return Optional.empty();
        }
        // a pruned column has the same values as the column
        return Optional.of(new VariantExtract(column.getBaseColumn(), path, cast.getType()));
    }

    private static boolean isSupportedType(Type type)
    {
        return type.equals(TINYINT) ||
                type.equals(SMALLINT) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(REAL) ||
                type.equals(DOUBLE) ||
                type.equals(BOOLEAN) ||
                type.equals(DATE) ||
                type.equals(VARCHAR) ||
                type instanceof DecimalType;
    }

    private record ExtractDomain(VariantExtract extract, Domain domain) {}
}

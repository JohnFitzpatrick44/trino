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
package io.trino.plugin.base.projection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Lambda;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.expression.StandardFunctions.VARIANT_PRUNE_FUNCTION_NAME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class ApplyProjectionUtil
{
    private ApplyProjectionUtil() {}

    public static List<ConnectorExpression> extractSupportedProjectedColumns(ConnectorExpression expression)
    {
        return extractSupportedProjectedColumns(expression, _ -> true);
    }

    public static List<ConnectorExpression> extractSupportedProjectedColumns(ConnectorExpression expression, Predicate<ConnectorExpression> expressionPredicate)
    {
        return extractSupportedProjectedColumns(expression, expressionPredicate, false);
    }

    /// Extracts the projected columns, which are variables and chains of dereferences on a variable.
    /// If `variantPrunes` is true, a `$variant_prune` call on a projected column is also a projected column,
    /// see [io.trino.spi.expression.StandardFunctions#VARIANT_PRUNE_FUNCTION_NAME].
    public static List<ConnectorExpression> extractSupportedProjectedColumns(ConnectorExpression expression, Predicate<ConnectorExpression> expressionPredicate, boolean variantPrunes)
    {
        requireNonNull(expression, "expression is null");
        ImmutableList.Builder<ConnectorExpression> supportedSubExpressions = ImmutableList.builder();
        fillSupportedProjectedColumns(expression, supportedSubExpressions, expressionPredicate, variantPrunes, Set.of());
        return supportedSubExpressions.build();
    }

    private static void fillSupportedProjectedColumns(
            ConnectorExpression expression,
            ImmutableList.Builder<ConnectorExpression> supportedSubExpressions,
            Predicate<ConnectorExpression> expressionPredicate,
            boolean variantPrunes,
            Set<String> localVariables)
    {
        if (isPushdownSupported(expression, expressionPredicate, variantPrunes, localVariables)) {
            supportedSubExpressions.add(expression);
            return;
        }

        if (expression instanceof Lambda lambda) {
            Set<String> lambdaLocalVariables = localLambdaVariables(localVariables, lambda);
            fillSupportedProjectedColumns(lambda.getBody(), supportedSubExpressions, expressionPredicate, variantPrunes, lambdaLocalVariables);
            return;
        }

        // If the whole expression is not supported, look for a partially supported projection
        for (ConnectorExpression child : expression.getChildren()) {
            fillSupportedProjectedColumns(child, supportedSubExpressions, expressionPredicate, variantPrunes, localVariables);
        }
    }

    @VisibleForTesting
    static boolean isPushdownSupported(ConnectorExpression expression, Predicate<ConnectorExpression> expressionPredicate, Set<String> localVariables)
    {
        return isPushdownSupported(expression, expressionPredicate, false, localVariables);
    }

    private static boolean isPushdownSupported(ConnectorExpression expression, Predicate<ConnectorExpression> expressionPredicate, boolean variantPrunes, Set<String> localVariables)
    {
        if (!expressionPredicate.test(expression)) {
            return false;
        }
        if (variantPrunes && isVariantPrune(expression)) {
            return isPushdownSupported(((Call) expression).getArguments().getFirst(), expressionPredicate, false, localVariables);
        }
        return (expression instanceof Variable variable && !localVariables.contains(variable.getName())) ||
                (expression instanceof FieldDereference fieldDereference
                        && isPushdownSupported(fieldDereference.getTarget(), expressionPredicate, false, localVariables));
    }

    private static boolean isVariantPrune(ConnectorExpression expression)
    {
        return expression instanceof Call call &&
                call.getFunctionName().equals(VARIANT_PRUNE_FUNCTION_NAME) &&
                call.getArguments().size() == 2 &&
                call.getArguments().get(1) instanceof Constant;
    }

    public static ProjectedColumnRepresentation createProjectedColumnRepresentation(ConnectorExpression expression)
    {
        List<List<String>> variantPaths = ImmutableList.of();
        if (isVariantPrune(expression)) {
            Call call = (Call) expression;
            variantPaths = variantPaths((Constant) call.getArguments().get(1));
            expression = call.getArguments().getFirst();
        }

        ImmutableList.Builder<Integer> ordinals = ImmutableList.builder();

        Variable target;
        while (true) {
            if (expression instanceof Variable variable) {
                target = variable;
                break;
            }
            if (expression instanceof FieldDereference dereference) {
                ordinals.add(dereference.getField());
                expression = dereference.getTarget();
            }
            else {
                throw new IllegalArgumentException("expression is not a valid dereference chain");
            }
        }

        return new ProjectedColumnRepresentation(target, ordinals.build().reverse(), variantPaths);
    }

    private static List<List<String>> variantPaths(Constant constant)
    {
        ArrayType pathsType = (ArrayType) constant.getType();
        ArrayType pathType = (ArrayType) pathsType.getElementType();
        Block paths = (Block) constant.getValue();
        ImmutableList.Builder<List<String>> result = ImmutableList.builder();
        for (int position = 0; position < paths.getPositionCount(); position++) {
            Block path = pathType.getObject(paths, position);
            ImmutableList.Builder<String> names = ImmutableList.builder();
            for (int index = 0; index < path.getPositionCount(); index++) {
                names.add(VARCHAR.getSlice(path, index).toStringUtf8());
            }
            result.add(names.build());
        }
        return result.build();
    }

    /**
     * Replace all connector expressions with variables as given by {@code expressionToVariableMappings} in a top down manner.
     * i.e. if the replacement occurs for the parent, the children will not be visited.
     */
    public static ConnectorExpression replaceWithNewVariables(ConnectorExpression expression, Map<ConnectorExpression, Variable> expressionToVariableMappings)
    {
        return replaceWithNewVariables(expression, expressionToVariableMappings, Set.of());
    }

    private static ConnectorExpression replaceWithNewVariables(ConnectorExpression expression, Map<ConnectorExpression, Variable> expressionToVariableMappings, Set<String> localVariables)
    {
        if (expressionToVariableMappings.containsKey(expression)) {
            if (isLocalProjection(expression, localVariables)) {
                return expression;
            }
            return expressionToVariableMappings.get(expression);
        }

        if (expression instanceof Constant || expression instanceof Variable) {
            return expression;
        }

        if (expression instanceof FieldDereference fieldDereference) {
            ConnectorExpression newTarget = replaceWithNewVariables(fieldDereference.getTarget(), expressionToVariableMappings, localVariables);
            return new FieldDereference(expression.getType(), newTarget, fieldDereference.getField());
        }

        if (expression instanceof Lambda lambda) {
            Set<String> lambdaLocalVariables = localLambdaVariables(localVariables, lambda);
            ConnectorExpression newBody = replaceWithNewVariables(lambda.getBody(), expressionToVariableMappings, lambdaLocalVariables);
            return new Lambda(lambda.getType(), lambda.getArguments(), newBody);
        }

        if (expression instanceof Call call) {
            return new Call(
                    call.getType(),
                    call.getFunctionName(),
                    call.getArguments().stream()
                            .map(argument -> replaceWithNewVariables(argument, expressionToVariableMappings, localVariables))
                            .collect(toImmutableList()));
        }

        // We cannot skip processing for unsupported expression shapes. This may lead to variables being left in ProjectionApplicationResult
        // which are no longer bound.
        throw new UnsupportedOperationException("Unsupported expression: " + expression);
    }

    private static boolean isLocalProjection(ConnectorExpression expression, Set<String> localVariables)
    {
        return switch (expression) {
            case Variable variable -> localVariables.contains(variable.getName());
            case FieldDereference field -> isLocalProjection(field.getTarget(), localVariables);
            default -> false;
        };
    }

    private static Set<String> localLambdaVariables(Set<String> localVariables, Lambda lambda)
    {
        Set<String> lambdaLocalVariables = new HashSet<>(localVariables);
        lambda.getArguments().stream()
                .map(Variable::getName)
                .forEach(lambdaLocalVariables::add);
        return lambdaLocalVariables;
    }

    public static class ProjectedColumnRepresentation
    {
        private final Variable variable;
        private final List<Integer> dereferenceIndices;
        private final List<List<String>> variantPaths;

        public ProjectedColumnRepresentation(Variable variable, List<Integer> dereferenceIndices)
        {
            this(variable, dereferenceIndices, ImmutableList.of());
        }

        public ProjectedColumnRepresentation(Variable variable, List<Integer> dereferenceIndices, List<List<String>> variantPaths)
        {
            this.variable = requireNonNull(variable, "variable is null");
            this.dereferenceIndices = ImmutableList.copyOf(requireNonNull(dereferenceIndices, "dereferenceIndices is null"));
            this.variantPaths = requireNonNull(variantPaths, "variantPaths is null").stream()
                    .map(ImmutableList::copyOf)
                    .collect(toImmutableList());
        }

        public Variable getVariable()
        {
            return variable;
        }

        public List<Integer> getDereferenceIndices()
        {
            return dereferenceIndices;
        }

        /// Returns the field name paths of a pruned variant, or an empty list if the column is not a pruned variant.
        public List<List<String>> getVariantPaths()
        {
            return variantPaths;
        }

        public boolean isVariable()
        {
            return dereferenceIndices.isEmpty() && variantPaths.isEmpty();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            ProjectedColumnRepresentation that = (ProjectedColumnRepresentation) obj;
            return Objects.equals(variable, that.variable) &&
                    Objects.equals(dereferenceIndices, that.dereferenceIndices) &&
                    Objects.equals(variantPaths, that.variantPaths);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(variable, dereferenceIndices, variantPaths);
        }
    }
}

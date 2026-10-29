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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.type.VariantFunctions.VARIANT_PRUNE_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

/// Replaces a VARIANT column that is only accessed by field name subscripts with a pruned variant,
/// so that the connector can read only the parts of the variant needed for the subscripts.
///
/// Transforms:
/// ```
/// Project(x := CAST(v['a']['b'] AS bigint))
///     Filter(CAST(v['c'] AS varchar) = 'c')
///         TableScan(v)
/// ```
/// to:
/// ```
/// Project(x := CAST(p['a']['b'] AS bigint))
///     Filter(CAST(p['c'] AS varchar) = 'c')
///         Project(p := $variant_prune(v, ARRAY[ARRAY['a', 'b'], ARRAY['c']]))
///             TableScan(v)
/// ```
///
/// `$variant_prune` never fails, so it can be evaluated before the filter. The subscripts and casts are not
/// moved, because they can fail. [RemoveVariantPrunes] removes prunes that were not pushed into the connector.
public final class PruneVariantPaths
{
    private static final ArrayType PATH_TYPE = new ArrayType(VARCHAR);
    private static final ArrayType PATHS_TYPE = new ArrayType(PATH_TYPE);

    private final PlannerContext plannerContext;

    public PruneVariantPaths(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(pruneProject(), pruneProjectAboveFilter());
    }

    Rule<ProjectNode> pruneProject()
    {
        return new PruneProject();
    }

    Rule<ProjectNode> pruneProjectAboveFilter()
    {
        return new PruneProjectAboveFilter();
    }

    private final class PruneProject
            implements Rule<ProjectNode>
    {
        private static final Pattern<ProjectNode> PATTERN = project()
                .with(source().matching(tableScan()));

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(ProjectNode project, Captures captures, Context context)
        {
            PlanNode source = context.getLookup().resolve(project.getSource());
            // A prune that was not pushed down was rejected by the connector, so it is not pruned again
            Map<Symbol, List<List<Slice>>> paths = prunablePaths(source, project.getAssignments().expressions(), false);
            if (paths.isEmpty()) {
                return Result.empty();
            }
            Map<Symbol, Symbol> pruned = prunedSymbols(paths, context);
            return Result.ofPlanNode(new ProjectNode(
                    project.getId(),
                    pruneProject(source, paths, pruned, context),
                    project.getAssignments().rewrite(expression -> rewrite(expression, pruned))));
        }
    }

    private final class PruneProjectAboveFilter
            implements Rule<ProjectNode>
    {
        private static final Capture<FilterNode> FILTER = newCapture();
        private static final Pattern<ProjectNode> PATTERN = project()
                .with(source().matching(filter().capturedAs(FILTER)
                        .with(source().matching(tableScan()))));

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(ProjectNode project, Captures captures, Context context)
        {
            FilterNode filter = captures.get(FILTER);
            PlanNode source = context.getLookup().resolve(filter.getSource());
            List<Expression> expressions = ImmutableList.<Expression>builder()
                    .addAll(project.getAssignments().expressions())
                    .add(filter.getPredicate())
                    .build();
            // A prune above the filter may come from a prune below the filter that was moved by predicate pushdown
            Map<Symbol, List<List<Slice>>> paths = prunablePaths(source, expressions, true);
            if (paths.isEmpty()) {
                return Result.empty();
            }
            Map<Symbol, Symbol> pruned = prunedSymbols(paths, context);
            return Result.ofPlanNode(new ProjectNode(
                    project.getId(),
                    new FilterNode(
                            filter.getId(),
                            pruneProject(source, paths, pruned, context),
                            rewrite(filter.getPredicate(), pruned)),
                    project.getAssignments().rewrite(expression -> rewrite(expression, pruned))));
        }
    }

    private static Map<Symbol, Symbol> prunedSymbols(Map<Symbol, List<List<Slice>>> paths, Rule.Context context)
    {
        ImmutableMap.Builder<Symbol, Symbol> pruned = ImmutableMap.builder();
        for (Symbol symbol : paths.keySet()) {
            pruned.put(symbol, context.getSymbolAllocator().newSymbol(symbol.name(), VARIANT));
        }
        return pruned.buildOrThrow();
    }

    private PlanNode pruneProject(PlanNode source, Map<Symbol, List<List<Slice>>> paths, Map<Symbol, Symbol> pruned, Rule.Context context)
    {
        ResolvedFunction prune = plannerContext.getMetadata().resolveBuiltinFunction(
                getCharVarcharCoercion(context.getSession()),
                VARIANT_PRUNE_FUNCTION_NAME,
                ImmutableList.of(VARIANT, PATHS_TYPE));
        Assignments.Builder assignments = Assignments.builder();
        for (Symbol symbol : source.getOutputSymbols()) {
            if (!paths.containsKey(symbol)) {
                assignments.putIdentity(symbol);
            }
        }
        paths.forEach((symbol, symbolPaths) -> assignments.put(
                pruned.get(symbol),
                new Call(prune, ImmutableList.of(symbol.toSymbolReference(), new Constant(PATHS_TYPE, pathsBlock(symbolPaths))))));
        return new ProjectNode(context.getIdAllocator().getNextId(), source, assignments.build());
    }

    private static Block pathsBlock(List<List<Slice>> paths)
    {
        BlockBuilder builder = PATH_TYPE.createBlockBuilder(null, paths.size());
        for (List<Slice> path : paths) {
            ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> path.forEach(name -> VARCHAR.writeSlice(elementBuilder, name)));
        }
        return builder.build();
    }

    private static Expression rewrite(Expression expression, Map<Symbol, Symbol> pruned)
    {
        // the prunes of the pruned columns are replaced by the new prune below the filter
        expression = ExpressionTreeRewriter.rewriteWith(new io.trino.sql.ir.ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (isPruneOfReference(node) && pruned.containsKey(Symbol.from(node.arguments().getFirst()))) {
                    return node.arguments().getFirst();
                }
                return treeRewriter.defaultRewrite(node, context);
            }
        }, expression);
        Map<Expression, Expression> mappings = new HashMap<>();
        pruned.forEach((symbol, prunedSymbol) -> mappings.put(symbol.toSymbolReference(), prunedSymbol.toSymbolReference()));
        return replaceExpression(expression, mappings);
    }

    /// Returns the paths of the VARIANT symbols of the source that are only used as the base of subscripts by field name.
    private static Map<Symbol, List<List<Slice>>> prunablePaths(PlanNode source, Collection<Expression> expressions, boolean prunedBase)
    {
        Set<Symbol> candidates = new HashSet<>();
        for (Symbol symbol : source.getOutputSymbols()) {
            if (symbol.type().equals(VARIANT)) {
                candidates.add(symbol);
            }
        }
        if (candidates.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Symbol, List<List<Slice>>> paths = new LinkedHashMap<>();
        Set<Symbol> disqualified = new HashSet<>();
        for (Expression expression : expressions) {
            collectPaths(expression, candidates, paths, disqualified, prunedBase);
        }
        disqualified.forEach(paths::remove);

        ImmutableMap.Builder<Symbol, List<List<Slice>>> result = ImmutableMap.builder();
        paths.forEach((symbol, symbolPaths) -> result.put(symbol, minimalPaths(symbolPaths)));
        return result.buildOrThrow();
    }

    private static void collectPaths(Expression expression, Set<Symbol> candidates, Map<Symbol, List<List<Slice>>> paths, Set<Symbol> disqualified, boolean prunedBase)
    {
        Optional<VariantAccess> access = variantAccess(expression, prunedBase);
        if (access.isPresent()) {
            Symbol symbol = Symbol.from(access.get().base());
            if (candidates.contains(symbol)) {
                if (access.get().path().isEmpty()) {
                    // the first subscript is by index, so the whole value is needed
                    disqualified.add(symbol);
                }
                else {
                    paths.computeIfAbsent(symbol, _ -> new ArrayList<>()).add(access.get().path());
                }
            }
            return;
        }
        if (expression instanceof Reference reference) {
            Symbol symbol = Symbol.from(reference);
            if (candidates.contains(symbol)) {
                disqualified.add(symbol);
            }
            return;
        }
        if (expression instanceof Lambda) {
            // the column may be used in the lambda body in any way
            for (Symbol symbol : extractUnique(expression)) {
                if (candidates.contains(symbol)) {
                    disqualified.add(symbol);
                }
            }
            return;
        }
        for (Expression child : expression.children()) {
            collectPaths(child, candidates, paths, disqualified, prunedBase);
        }
    }

    /// Matches a chain of VARIANT subscripts on a column reference. The path contains the leading subscripts by field name.
    private static Optional<VariantAccess> variantAccess(Expression expression, boolean prunedBase)
    {
        List<Optional<Slice>> keys = new ArrayList<>();
        Expression current = expression;
        while (isVariantSubscript(current)) {
            Call call = (Call) current;
            keys.addFirst(fieldName(call.arguments().get(1)));
            current = call.arguments().getFirst();
        }
        // a prune of a column that was not pushed into the table scan, for example because the filter
        // was pushed below the prune, is pruned again below the filter
        if (prunedBase && isPruneOfReference(current)) {
            current = ((Call) current).arguments().getFirst();
        }
        if (keys.isEmpty() || !(current instanceof Reference reference)) {
            return Optional.empty();
        }
        ImmutableList.Builder<Slice> path = ImmutableList.builder();
        for (Optional<Slice> key : keys) {
            if (key.isEmpty()) {
                break;
            }
            path.add(key.get());
        }
        return Optional.of(new VariantAccess(reference, path.build()));
    }

    private static boolean isPruneOfReference(Expression expression)
    {
        return expression instanceof Call call &&
                call.function().name().equals(builtinFunctionName(VARIANT_PRUNE_FUNCTION_NAME)) &&
                call.arguments().getFirst() instanceof Reference;
    }

    private static boolean isVariantSubscript(Expression expression)
    {
        return expression instanceof Call call &&
                call.function().name().equals(builtinFunctionName(SUBSCRIPT)) &&
                call.arguments().size() == 2 &&
                call.arguments().getFirst().type().equals(VARIANT);
    }

    private static Optional<Slice> fieldName(Expression key)
    {
        if (key instanceof Constant constant && constant.type() instanceof VarcharType && constant.value() != null) {
            return Optional.of((Slice) constant.value());
        }
        return Optional.empty();
    }

    /// Removes duplicate paths and paths that have another path as a prefix
    private static List<List<Slice>> minimalPaths(List<List<Slice>> paths)
    {
        List<List<Slice>> sorted = new ArrayList<>(paths);
        sorted.sort((left, right) -> Integer.compare(left.size(), right.size()));
        List<List<Slice>> result = new ArrayList<>();
        for (List<Slice> path : sorted) {
            boolean covered = result.stream().anyMatch(prefix -> path.subList(0, prefix.size()).equals(prefix));
            if (!covered) {
                result.add(path);
            }
        }
        return ImmutableList.copyOf(result);
    }

    private record VariantAccess(Reference base, List<Slice> path) {}
}

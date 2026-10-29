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
import io.trino.metadata.ResolvedFunction;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.MatchContext;
import io.trino.sql.planner.assertions.MatchResult;
import io.trino.sql.planner.assertions.Matcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.VariantFunctions.VARIANT_PRUNE_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPruneVariantPaths
        extends BaseRuleTest
{
    @Test
    void testProjectAboveTableScan()
    {
        tester().assertThat(rules().pruneProject())
                .on(p -> {
                    Symbol variant = p.symbol("v", VARIANT);
                    Symbol other = p.symbol("k", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(p.symbol("x", BIGINT), new Cast(subscript(subscript(variant.toSymbolReference(), "a"), "b"), BIGINT))
                                    .put(p.symbol("y", VARIANT), subscript(subscript(variant.toSymbolReference(), "a"), 1))
                                    .put(p.symbol("z", VARIANT), subscript(variant.toSymbolReference(), "c"))
                                    .putIdentity(other)
                                    .build(),
                            tableScan(p, variant, other));
                })
                .matches(project(project(node(TableScanNode.class)).with(new PruneMatcher(ImmutableSet.of(ImmutableList.of("a"), ImmutableList.of("c"))))));
    }

    @Test
    void testProjectAboveFilterAboveTableScan()
    {
        tester().assertThat(rules().pruneProjectAboveFilter())
                .on(p -> {
                    Symbol variant = p.symbol("v", VARIANT);
                    Symbol other = p.symbol("k", BIGINT);
                    return p.project(
                            Assignments.builder()
                                    .put(p.symbol("x", BIGINT), new Cast(subscript(subscript(variant.toSymbolReference(), "a"), "b"), BIGINT))
                                    .putIdentity(other)
                                    .build(),
                            p.filter(
                                    comparison(EQUAL, new Cast(subscript(variant.toSymbolReference(), "c"), VARCHAR), new Constant(VARCHAR, utf8Slice("x"))),
                                    tableScan(p, variant, other)));
                })
                .matches(project(node(FilterNode.class, project(node(TableScanNode.class)).with(new PruneMatcher(ImmutableSet.of(ImmutableList.of("a", "b"), ImmutableList.of("c")))))));
    }

    @Test
    void testPrunesUnpushedPrune()
    {
        // a prune that was not pushed into the table scan, for example because the filter was pushed below it, is pruned below the filter
        tester().assertThat(rules().pruneProjectAboveFilter())
                .on(p -> {
                    Symbol variant = p.symbol("v", VARIANT);
                    Symbol other = p.symbol("k", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("x", BIGINT), new Cast(subscript(prune(variant.toSymbolReference()), "a"), BIGINT)),
                            p.filter(
                                    comparison(EQUAL, other.toSymbolReference(), new Constant(BIGINT, 1L)),
                                    tableScan(p, variant, other)));
                })
                .matches(project(node(FilterNode.class, project(node(TableScanNode.class)).with(new PruneMatcher(ImmutableSet.of(ImmutableList.of("a")))))));
    }

    @Test
    void testDoesNotFireWhenWholeVariantIsUsed()
    {
        tester().assertThat(rules().pruneProject())
                .on(p -> {
                    Symbol variant = p.symbol("v", VARIANT);
                    return p.project(
                            Assignments.builder()
                                    .put(p.symbol("x", VARIANT), subscript(variant.toSymbolReference(), "a"))
                                    .putIdentity(variant)
                                    .build(),
                            tableScan(p, variant));
                })
                .doesNotFire();

        tester().assertThat(rules().pruneProjectAboveFilter())
                .on(p -> {
                    Symbol variant = p.symbol("v", VARIANT);
                    return p.project(
                            Assignments.of(p.symbol("x", VARIANT), subscript(variant.toSymbolReference(), "a")),
                            p.filter(
                                    new Call(tester().getMetadata().resolveBuiltinFunction(getCharVarcharCoercion(testSessionBuilder().build()), "variant_is_null", ImmutableList.of(VARIANT)), ImmutableList.of(variant.toSymbolReference())),
                                    tableScan(p, variant)));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForIndexSubscript()
    {
        tester().assertThat(rules().pruneProject())
                .on(p -> {
                    Symbol variant = p.symbol("v", VARIANT);
                    return p.project(
                            Assignments.of(p.symbol("x", VARIANT), subscript(subscript(variant.toSymbolReference(), 1), "a")),
                            tableScan(p, variant));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForLambda()
    {
        tester().assertThat(rules().pruneProject())
                .on(p -> {
                    Symbol variant = p.symbol("v", VARIANT);
                    Symbol argument = p.symbol("arg", BIGINT);
                    return p.project(
                            Assignments.of(p.symbol("x", new FunctionType(ImmutableList.of(BIGINT), VARIANT)), new Lambda(ImmutableList.of(argument), subscript(variant.toSymbolReference(), "a"))),
                            tableScan(p, variant));
                })
                .doesNotFire();
    }

    @Test
    void testRemoveVariantPrunes()
    {
        Reference variant = new Reference(VARIANT, "v");
        ResolvedFunction prune = tester().getMetadata().resolveBuiltinFunction(
                getCharVarcharCoercion(testSessionBuilder().build()),
                VARIANT_PRUNE_FUNCTION_NAME,
                ImmutableList.of(VARIANT, new ArrayType(new ArrayType(VARCHAR))));
        Expression expression = subscript(new Call(prune, ImmutableList.of(variant, new Constant(new ArrayType(new ArrayType(VARCHAR)), null))), "a");
        assertThat(RemoveVariantPrunes.removePrunes(expression)).isEqualTo(subscript(variant, "a"));
    }

    private Call prune(Expression variant)
    {
        ArrayType pathsType = new ArrayType(new ArrayType(VARCHAR));
        ResolvedFunction prune = tester().getMetadata().resolveBuiltinFunction(
                getCharVarcharCoercion(testSessionBuilder().build()),
                VARIANT_PRUNE_FUNCTION_NAME,
                ImmutableList.of(VARIANT, pathsType));
        return new Call(prune, ImmutableList.of(variant, new Constant(pathsType, pathType().createBlockBuilder(null, 0).build())));
    }

    private static ArrayType pathType()
    {
        return new ArrayType(VARCHAR);
    }

    private PruneVariantPaths rules()
    {
        return new PruneVariantPaths(tester().getPlannerContext());
    }

    private static PlanNode tableScan(io.trino.sql.planner.iterative.rule.test.PlanBuilder p, Symbol... symbols)
    {
        ImmutableMap.Builder<Symbol, io.trino.spi.connector.ColumnHandle> assignments = ImmutableMap.builder();
        for (Symbol symbol : symbols) {
            assignments.put(symbol, new TpchColumnHandle(symbol.name(), symbol.type()));
        }
        return p.tableScan(ImmutableList.copyOf(symbols), assignments.buildOrThrow());
    }

    private Call subscript(Expression base, String fieldName)
    {
        return subscript(base, new Constant(VARCHAR, utf8Slice(fieldName)));
    }

    private Call subscript(Expression base, long index)
    {
        return subscript(base, new Constant(BIGINT, index));
    }

    private Call subscript(Expression base, Constant key)
    {
        List<Type> types = ImmutableList.of(VARIANT, key.type());
        return new Call(tester().getMetadata().resolveOperator(getCharVarcharCoercion(testSessionBuilder().build()), SUBSCRIPT, types), ImmutableList.of(base, key));
    }

    /// Matches a project that prunes a variant to the paths
    private record PruneMatcher(Set<List<String>> expectedPaths)
            implements Matcher
    {
        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof ProjectNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, MatchContext context)
        {
            for (Expression expression : ((ProjectNode) node).getAssignments().expressions()) {
                if (expression instanceof Call call && call.function().name().equals(builtinFunctionName(VARIANT_PRUNE_FUNCTION_NAME))) {
                    Block paths = (Block) ((Constant) call.arguments().get(1)).value();
                    ImmutableSet.Builder<List<String>> actualPaths = ImmutableSet.builder();
                    ArrayType pathType = new ArrayType(VARCHAR);
                    for (int position = 0; position < paths.getPositionCount(); position++) {
                        Block path = pathType.getObject(paths, position);
                        ImmutableList.Builder<String> names = ImmutableList.builder();
                        for (int index = 0; index < path.getPositionCount(); index++) {
                            names.add(VARCHAR.getSlice(path, index).toStringUtf8());
                        }
                        actualPaths.add(names.build());
                    }
                    if (actualPaths.build().equals(expectedPaths)) {
                        return MatchResult.match();
                    }
                }
            }
            return MatchResult.NO_MATCH;
        }
    }
}

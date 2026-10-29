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

import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.type.VariantFunctions.VARIANT_PRUNE_FUNCTION_NAME;

/// Removes the `$variant_prune` calls that were not pushed into a connector. A prune returns its argument
/// unchanged, so removing it avoids evaluating it.
public class RemoveVariantPrunes
        extends ExpressionRewriteRuleSet
{
    public RemoveVariantPrunes()
    {
        super((expression, _) -> removePrunes(expression));
    }

    static Expression removePrunes(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new io.trino.sql.ir.ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (node.function().name().equals(builtinFunctionName(VARIANT_PRUNE_FUNCTION_NAME))) {
                    return treeRewriter.rewrite(node.arguments().getFirst(), context);
                }
                return treeRewriter.defaultRewrite(node, context);
            }
        }, expression);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.InsertEmptyBuckets;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.Map;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

/**
 * Handles a {@code BUCKET} whose {@code from}..{@code to} range is invalid, as reported by {@link Bucket#invalidBounds}:
 * a null bound, a non-finite numeric bound, a numeric {@code to} that is not greater than {@code from}, or a date
 * {@code to} before {@code from}. Such a range comes from Kibana filters, query parameters or values propagated from
 * an {@code EVAL} as easily as from a typo, so it is not a planning error: the query runs and a response-header warning
 * names the problem.
 * <ul>
 *     <li>A {@code BUCKET} that picks its bucket size from a target bucket count, or that has a null bound, has no
 *     meaningful bucket values. It is replaced by a null literal and the warning ends in {@code "returning null"}.</li>
 *     <li>A {@code BUCKET} with a fixed date span uses its range only to insert empty buckets, so its values stay as
 *     they are. With {@code {"include_empty_buckets": true}} and {@code to} before {@code from}, no empty buckets are
 *     inserted and the warning ends in {@code "empty buckets are not inserted"}.</li>
 * </ul>
 * {@link InsertEmptyBuckets} keeps its own copy of each {@code BUCKET}, which no expression rule rewrites. So this rule
 * decides each of its buckets from the grouping's current expression below it: a grouping that is now a null literal,
 * or a {@code BUCKET} with an invalid range, becomes a plain group, and the node is dropped once it has no buckets left.
 * A grouping that is still a valid {@code BUCKET} replaces the copy, so the copy sees bounds folded since it was made.
 * <p>
 * This runs before {@link FoldNull} in {@code LogicalPlanOptimizer.operators()}: {@code BUCKET} is
 * {@link org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull}, so {@link FoldNull} would otherwise turn a null
 * bound into a null result without a warning. A bound that only becomes a constant through {@link PropagateEvalFoldables}
 * or {@link ConstantFolding} is seen on a later pass of the batch.
 */
public final class ReplaceInvalidBucketBounds extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext ctx) {
        FoldContext foldCtx = ctx.foldCtx();
        plan = plan.transformExpressionsUp(Bucket.class, bucket -> replaceInvalidBucket(bucket, foldCtx));
        return plan.transformUp(InsertEmptyBuckets.class, insertEmptyBuckets -> dropInvalidBuckets(insertEmptyBuckets, foldCtx));
    }

    private static Expression replaceInvalidBucket(Bucket bucket, FoldContext foldCtx) {
        String reason = bucket.invalidBounds(foldCtx);
        if (reason == null) {
            return bucket;
        }
        if (DataType.isTemporalAmount(bucket.buckets().dataType()) && hasNullBound(bucket, foldCtx) == false) {
            // A fixed span does not depend on the range; InsertEmptyBuckets handles the range.
            return bucket;
        }
        warn(bucket, reason, "returning null");
        return Literal.of(bucket, null);
    }

    private static boolean hasNullBound(Bucket bucket, FoldContext foldCtx) {
        return Foldables.valueOf(foldCtx, bucket.from()) == null || Foldables.valueOf(foldCtx, bucket.to()) == null;
    }

    private static LogicalPlan dropInvalidBuckets(InsertEmptyBuckets insertEmptyBuckets, FoldContext foldCtx) {
        AttributeMap<Expression> aliases = InsertEmptyBucketsAfterAggregate.collectAliases(insertEmptyBuckets.child());
        AttributeMap.Builder<Bucket> buckets = AttributeMap.builder();
        AttributeSet.Builder groups = AttributeSet.builder().addAll(insertEmptyBuckets.groups());
        boolean changed = false;
        for (Map.Entry<Attribute, Bucket> entry : insertEmptyBuckets.buckets().entrySet()) {
            Attribute attribute = entry.getKey();
            Bucket copy = entry.getValue();
            Expression current = aliases.resolve(attribute);
            if (current instanceof Bucket bucket) {
                String reason = bucket.invalidBounds(foldCtx);
                if (reason == null) {
                    buckets.put(attribute, bucket);
                    changed |= bucket.equals(copy) == false;
                } else {
                    warn(bucket, reason, "empty buckets are not inserted");
                    groups.add(attribute);
                    changed = true;
                }
            } else if (current instanceof Literal literal && literal.value() == null) {
                // The BUCKET was folded to null, here or by FoldNull; every row keeps that null.
                groups.add(attribute);
                changed = true;
            } else {
                buckets.put(attribute, copy);
            }
        }
        if (changed == false) {
            return insertEmptyBuckets;
        }
        AttributeMap<Bucket> remaining = buckets.build();
        if (remaining.isEmpty()) {
            return insertEmptyBuckets.child();
        }
        return new InsertEmptyBuckets(
            insertEmptyBuckets.source(),
            insertEmptyBuckets.child(),
            remaining,
            groups.build(),
            insertEmptyBuckets.defaultValues()
        );
    }

    private static void warn(Bucket bucket, String reason, String outcome) {
        Source source = bucket.source();
        addWarning(
            "Line {}:{}: [{}] has invalid bounds: {}; {}",
            source.source().getLineNumber(),
            source.source().getColumnNumber(),
            source.text(),
            reason,
            outcome
        );
    }
}

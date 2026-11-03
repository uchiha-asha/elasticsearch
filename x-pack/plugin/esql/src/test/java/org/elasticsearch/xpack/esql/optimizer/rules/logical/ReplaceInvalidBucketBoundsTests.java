/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.InsertEmptyBuckets;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Runs the whole logical optimizer, so the tests also pin that {@link ReplaceInvalidBucketBounds} sees a {@code BUCKET}
 * with a null bound before {@link FoldNull} folds it without a warning, and that it sees bounds propagated from an
 * {@code EVAL}.
 */
public class ReplaceInvalidBucketBoundsTests extends AbstractLogicalPlanOptimizerTests {

    public ReplaceInvalidBucketBoundsTests(VersionMode versionMode) {
        super(versionMode);
    }

    private static List<Bucket> buckets(LogicalPlan plan) {
        List<Bucket> buckets = new ArrayList<>();
        plan.forEachDown(p -> p.forEachExpressionDown(Bucket.class, buckets::add));
        return buckets;
    }

    private static List<InsertEmptyBuckets> insertEmptyBuckets(LogicalPlan plan) {
        return plan.collect(InsertEmptyBuckets.class);
    }

    public void testNumericToBeforeFromReturnsNull() {
        LogicalPlan plan = plan("FROM test | STATS c = COUNT(*) BY b = BUCKET(salary, 10, 50000, 10000)");
        assertThat(buckets(plan), empty());
        assertWarnings(
            "Line 1:39: [BUCKET(salary, 10, 50000, 10000)] has invalid bounds: [to] [10000] must be greater than [from] [50000]; "
                + "returning null"
        );
    }

    public void testPropagatedNullBoundReturnsNullWithWarning() {
        LogicalPlan plan = plan("FROM test | EVAL t = null::integer | STATS c = COUNT(*) BY b = BUCKET(salary, 10, 0, t)");
        assertThat(buckets(plan), empty());
        assertWarnings("Line 1:64: [BUCKET(salary, 10, 0, t)] has invalid bounds: [to] is null; returning null");
    }

    public void testValidBoundsAreKept() {
        LogicalPlan plan = plan("FROM test | STATS c = COUNT(*) BY b = BUCKET(salary, 10, 0, 100000)");
        assertThat(buckets(plan), not(empty()));
        ensureNoWarnings();
    }

    public void testValidBoundsKeepEmptyBuckets() {
        LogicalPlan plan = plan("FROM test | STATS c = COUNT(*) BY b = BUCKET(salary, 10, 0, 100000, {\"include_empty_buckets\": true})");
        assertThat(insertEmptyBuckets(plan), hasSize(1));
        ensureNoWarnings();
    }

    public void testIncludeEmptyBucketsWithInvalidBoundsDropsEmptyBuckets() {
        LogicalPlan plan = plan("FROM test | STATS c = COUNT(*) BY b = BUCKET(salary, 10, 100, 100, {\"include_empty_buckets\": true})");
        assertThat(buckets(plan), empty());
        assertThat(insertEmptyBuckets(plan), empty());
        assertWarnings(
            "Line 1:39: [BUCKET(salary, 10, 100, 100, {\"include_empty_buckets\": true})] has invalid bounds: "
                + "[to] [100] must be greater than [from] [100]; returning null"
        );
    }

    /**
     * The copy of the {@code BUCKET} inside {@link InsertEmptyBuckets} still has the unpropagated {@code t}, so this needs
     * the decision to come from the grouping below it.
     */
    public void testIncludeEmptyBucketsWithPropagatedNullBoundDropsEmptyBuckets() {
        LogicalPlan plan = plan(
            "FROM test | EVAL t = null::integer | STATS c = COUNT(*) BY b = BUCKET(salary, 10, 0, t, {\"include_empty_buckets\": true})"
        );
        assertThat(buckets(plan), empty());
        assertThat(insertEmptyBuckets(plan), empty());
        assertWarnings(
            "Line 1:64: [BUCKET(salary, 10, 0, t, {\"include_empty_buckets\": true})] has invalid bounds: [to] is null; returning null"
        );
    }

    public void testInvalidBucketBecomesGroupNextToValidBucket() {
        LogicalPlan plan = plan(
            "FROM test | STATS c = COUNT(*) BY m = BUCKET(hire_date, 4, \"1985-01-01T00:00:00Z\", \"1985-05-01T00:00:00Z\", "
                + "{\"include_empty_buckets\": true}), b = BUCKET(salary, 10, 100, 100, {\"include_empty_buckets\": true})"
        );
        List<InsertEmptyBuckets> nodes = insertEmptyBuckets(plan);
        assertThat(nodes, hasSize(1));
        InsertEmptyBuckets node = nodes.get(0);
        assertThat(node.buckets().keySet().stream().map(Attribute::name).toList(), contains("m"));
        assertThat(node.groups().stream().map(Attribute::name).toList(), contains("b"));
        assertWarnings(
            "Line 1:146: [BUCKET(salary, 10, 100, 100, {\"include_empty_buckets\": true})] has invalid bounds: "
                + "[to] [100] must be greater than [from] [100]; returning null"
        );
    }

    /** A fixed span does not depend on the range, so only the empty buckets are dropped. */
    public void testSpanWithToBeforeFromKeepsBucketAndDropsEmptyBuckets() {
        LogicalPlan plan = plan(
            "FROM test | STATS c = COUNT(*) BY m = BUCKET(hire_date, 1 month, \"1985-06-01T00:00:00Z\", \"1985-01-01T00:00:00Z\", "
                + "{\"include_empty_buckets\": true})"
        );
        assertThat(buckets(plan), not(empty()));
        assertThat(insertEmptyBuckets(plan), empty());
        assertWarnings(
            "Line 1:39: [BUCKET(hire_date, 1 month, \"1985-06-01T00:00:00Z\", \"1985-01-01T00:00:00Z\", "
                + "{\"include_empty_buckets\": true})] has invalid bounds: [to] [1985-01-01T00:00:00.000Z] must not be before "
                + "[from] [1985-06-01T00:00:00.000Z]; empty buckets are not inserted"
        );
    }

    public void testSpanWithNullBoundReturnsNull() {
        LogicalPlan plan = plan(
            "FROM test | EVAL t = null::datetime | STATS c = COUNT(*) BY m = BUCKET(hire_date, 1 month, \"1985-01-01T00:00:00Z\", t)"
        );
        assertThat(buckets(plan), empty());
        assertWarnings(
            "Line 1:65: [BUCKET(hire_date, 1 month, \"1985-01-01T00:00:00Z\", t)] has invalid bounds: [to] is null; returning null"
        );
    }

    /** The copy inside {@link InsertEmptyBuckets} follows the grouping, so it sees the propagated bounds. */
    public void testPropagatedBoundsReachEmptyBuckets() {
        LogicalPlan plan = plan(
            "FROM test | EVAL f = 0, t = 100000 | STATS c = COUNT(*) BY b = BUCKET(salary, 10, f, t, {\"include_empty_buckets\": true})"
        );
        List<InsertEmptyBuckets> nodes = insertEmptyBuckets(plan);
        assertThat(nodes, hasSize(1));
        Map.Entry<Attribute, Bucket> entry = nodes.get(0).buckets().entrySet().iterator().next();
        Bucket bucket = entry.getValue();
        assertThat(bucket.from().foldable(), equalTo(true));
        assertThat(bucket.to().foldable(), equalTo(true));
        assertThat(bucket.invalidBounds(FoldContext.small()), nullValue());
        ensureNoWarnings();
    }
}

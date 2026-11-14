/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.aggregations.bucket.terms;

import org.elasticsearch.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(1_000_000)
@State(Scope.Benchmark)
public class LongKeyedBucketOrdsBenchmark {
    private static final long LIMIT = 1_000_000;
    /**
     * The number of distinct values to add to the buckets.
     */
    private static final long DISTINCT_VALUES = 210;
    /**
     * The number of buckets to create in the {@link #multiBucket} case.
     * <p>
     * If this is not relatively prime to {@link #DISTINCT_VALUES_IN_BUCKETS}
     * then the values won't be scattered evenly across the buckets.
     */
    private static final long DISTINCT_BUCKETS = 21;
    /**
     * Number of distinct values to add to values within buckets.
     */
    private static final long DISTINCT_VALUES_IN_BUCKETS = 10;
    /**
     * The number of buckets added by each {@link ManyOwners} shape.
     */
    private static final int OWNED_BUCKETS = 100_000;

    private final PageCacheRecycler recycler = new PageCacheRecycler(Settings.EMPTY);
    private final BigArrays bigArrays = new BigArrays(recycler, null, "REQUEST");

    /**
     * Force loading all of the implementations just for extra paranoia's sake.
     * We really don't want the JVM to be able to eliminate one of them just
     * because we don't use it in the particular benchmark. That is totally a
     * thing it'd do. It is sneaky.
     */
    @Setup
    public void forceLoadClasses(Blackhole bh) {
        bh.consume(LongKeyedBucketOrds.FromSingle.class);
        bh.consume(LongKeyedBucketOrds.FromMany.class);
        bh.consume(LongKeyedBucketOrds.FromManySmall.class);
    }

    /**
     * Emulates a way that we do <strong>not</strong> use {@link LongKeyedBucketOrds}
     * because it is not needed.
     */
    @Benchmark
    public void singleBucketIntoSingleImmutableMonmorphicInvocation(Blackhole bh) {
        try (LongKeyedBucketOrds.FromSingle ords = new LongKeyedBucketOrds.FromSingle(bigArrays)) {
            for (long i = 0; i < LIMIT; i++) {
                ords.add(0, i % DISTINCT_VALUES);
            }
            if (ords.size() != DISTINCT_VALUES) {
                throw new IllegalArgumentException("Expected [" + DISTINCT_VALUES + "] but found [" + ords.size() + "]");
            }
            bh.consume(ords);
        }
    }

    /**
     * Emulates the way that most aggregations use {@link LongKeyedBucketOrds}.
     */
    @Benchmark
    public void singleBucketIntoSingleImmutableMegamorphicInvocation(Blackhole bh) {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE)) {
            for (long i = 0; i < LIMIT; i++) {
                ords.add(0, i % DISTINCT_VALUES);
            }
            if (ords.size() != DISTINCT_VALUES) {
                throw new IllegalArgumentException("Expected [" + DISTINCT_VALUES + "] but found [" + ords.size() + "]");
            }
            bh.consume(ords);
        }
    }

    /**
     * Emulates the way that {@link AutoDateHistogramAggregationBuilder} uses {@link LongKeyedBucketOrds}.
     */
    @Benchmark
    public void singleBucketIntoSingleMutableMonmorphicInvocation(Blackhole bh) {
        LongKeyedBucketOrds.FromSingle ords = new LongKeyedBucketOrds.FromSingle(bigArrays);
        for (long i = 0; i < LIMIT; i++) {
            if (i % 100_000 == 0) {
                ords.close();
                bh.consume(ords);
                ords = new LongKeyedBucketOrds.FromSingle(bigArrays);
            }
            ords.add(0, i % DISTINCT_VALUES);
        }
        if (ords.size() != DISTINCT_VALUES) {
            ords.close();
            throw new IllegalArgumentException("Expected [" + DISTINCT_VALUES + "] but found [" + ords.size() + "]");
        }
        bh.consume(ords);
        ords.close();
    }

    /**
     * Emulates a way that we do <strong>not</strong> use {@link LongKeyedBucketOrds}
     * because it is significantly slower than the
     * {@link #singleBucketIntoSingleMutableMonmorphicInvocation monomorphic invocation}.
     */
    @Benchmark
    public void singleBucketIntoSingleMutableMegamorphicInvocation(Blackhole bh) {
        LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE);
        for (long i = 0; i < LIMIT; i++) {
            if (i % 100_000 == 0) {
                ords.close();
                bh.consume(ords);
                ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE);
            }
            ords.add(0, i % DISTINCT_VALUES);
        }
        if (ords.size() != DISTINCT_VALUES) {
            throw new IllegalArgumentException("Expected [" + DISTINCT_VALUES + "] but found [" + ords.size() + "]");
        }
        bh.consume(ords);
        ords.close();
    }

    /**
     * Emulates an aggregation that collects from a single bucket "by accident".
     * This can happen if an aggregation is under, say, a {@code terms}
     * aggregation and there is only a single value for that term in the index
     * but we can't tell that up front.
     */
    @Benchmark
    public void singleBucketIntoMulti(Blackhole bh) {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY)) {
            singleBucketIntoMultiSmall(ords);
            bh.consume(ords);
        }
    }

    /**
     * Emulates an aggregation that collects from a single bucket "by accident"
     * and gets a "small" bucket ords. This can happen to a {@code terms} inside
     * of another {@code terms} when the "inner" terms only even has a single
     * bucket.
     */
    @Benchmark
    public void singleBucketIntoMultiSmall(Blackhole bh) {
        try (LongKeyedBucketOrds ords = new LongKeyedBucketOrds.FromManySmall(bigArrays, 60)) {
            singleBucketIntoMultiSmall(ords);
            bh.consume(ords);
        }
    }

    private void singleBucketIntoMultiSmall(LongKeyedBucketOrds ords) {
        for (long i = 0; i < LIMIT; i++) {
            ords.add(0, i % DISTINCT_VALUES);
        }
        if (ords.size() != DISTINCT_VALUES) {
            throw new IllegalArgumentException("Expected [" + DISTINCT_VALUES + "] but found [" + ords.size() + "]");
        }
    }

    /**
     * Emulates an aggregation that collects from many buckets with a known
     * bounds on the values.
     */
    @Benchmark
    public void multiBucketManySmall(Blackhole bh) {
        try (LongKeyedBucketOrds ords = new LongKeyedBucketOrds.FromManySmall(bigArrays, 5)) {
            multiBucket(bh, ords);
        }
    }

    /**
     * Emulates an aggregation that collects from many buckets.
     */
    @Benchmark
    public void multiBucketMany(Blackhole bh) {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY)) {
            multiBucket(bh, ords);
        }
    }

    private void multiBucket(Blackhole bh, LongKeyedBucketOrds ords) {
        for (long i = 0; i < LIMIT; i++) {
            ords.add(i % DISTINCT_BUCKETS, i % DISTINCT_VALUES_IN_BUCKETS);
        }
        if (ords.size() != DISTINCT_VALUES) {
            throw new IllegalArgumentException("Expected [" + DISTINCT_VALUES + "] but found [" + ords.size() + "]");
        }
        bh.consume(ords);
    }

    /**
     * Shapes of owning buckets for the {@link #collectFromManyOwners collect}
     * and {@link #collectAndBuildFromManyOwners collect and build} benchmarks.
     * Each shape adds {@link #OWNED_BUCKETS} buckets.
     */
    @State(Scope.Benchmark)
    public static class ManyOwners {
        /**
         * {@code dense}: 1,000 owning buckets with 100 buckets each, all 1,000 are built.
         * {@code sparse}: 1,000 owning buckets spread over ordinals up to 10,000, all 10,000 are built.
         * {@code single}: one owning bucket with all buckets, 1,000 owning buckets are built.
         */
        @Param({ "dense", "sparse", "single" })
        public String shape;

        /**
         * {@code many} hashes the {@code owningBucketOrd, value} pair, {@code small} packs it into one {@code long}.
         */
        @Param({ "many", "small" })
        public String impl;

        long[] owners;
        long[] values;
        long builtOwners;

        @Setup
        public void setup() {
            owners = new long[OWNED_BUCKETS];
            values = new long[OWNED_BUCKETS];
            for (int i = 0; i < OWNED_BUCKETS; i++) {
                switch (shape) {
                    case "dense" -> {
                        owners[i] = i % 1_000;
                        values[i] = i / 1_000;
                    }
                    case "sparse" -> {
                        owners[i] = (i % 1_000) * 10L;
                        values[i] = i / 1_000;
                    }
                    case "single" -> {
                        owners[i] = 0;
                        values[i] = i;
                    }
                    default -> throw new IllegalArgumentException("unknown shape [" + shape + "]");
                }
            }
            builtOwners = switch (shape) {
                case "dense", "single" -> 1_000;
                case "sparse" -> 10_000;
                default -> throw new IllegalArgumentException("unknown shape [" + shape + "]");
            };
        }

        LongKeyedBucketOrds newOrds(BigArrays bigArrays) {
            return switch (impl) {
                case "many" -> new LongKeyedBucketOrds.FromMany(bigArrays);
                case "small" -> new LongKeyedBucketOrds.FromManySmall(bigArrays, 32);
                default -> throw new IllegalArgumentException("unknown impl [" + impl + "]");
            };
        }
    }

    /**
     * Collects {@link ManyOwners} without building results.
     */
    @Benchmark
    @OperationsPerInvocation(1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void collectFromManyOwners(ManyOwners state, Blackhole bh) {
        try (LongKeyedBucketOrds ords = state.newOrds(bigArrays)) {
            collect(state, ords);
            bh.consume(ords);
        }
    }

    /**
     * Collects {@link ManyOwners} and then reads them the way aggregations
     * build results: count the buckets of each owning bucket, then
     * enumerate each owning bucket's buckets.
     */
    @Benchmark
    @OperationsPerInvocation(1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void collectAndBuildFromManyOwners(ManyOwners state, Blackhole bh) {
        try (LongKeyedBucketOrds ords = state.newOrds(bigArrays)) {
            collect(state, ords);
            long total = 0;
            for (long owner = 0; owner < state.builtOwners; owner++) {
                total += ords.bucketsInOrd(owner);
            }
            for (long owner = 0; owner < state.builtOwners; owner++) {
                LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(owner);
                while (ordsEnum.next()) {
                    bh.consume(ordsEnum.value());
                    total--;
                }
            }
            if (total != 0) {
                throw new IllegalArgumentException("counted and enumerated buckets differ by [" + total + "]");
            }
            bh.consume(ords);
        }
    }

    private static void collect(ManyOwners state, LongKeyedBucketOrds ords) {
        for (int i = 0; i < OWNED_BUCKETS; i++) {
            ords.add(state.owners[i], state.values[i]);
        }
        if (ords.size() != OWNED_BUCKETS) {
            throw new IllegalArgumentException("Expected [" + OWNED_BUCKETS + "] but found [" + ords.size() + "]");
        }
    }
}

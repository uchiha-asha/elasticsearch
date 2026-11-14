/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;

import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.TreeSet;

/**
 * Maps owning bucket ordinals and long bucket keys to bucket ordinals.
 */
public abstract class LongKeyedBucketOrds implements Releasable {
    /**
     * Build a {@link LongKeyedBucketOrds} who's values have unknown bounds.
     *
     * @param cardinality - This should come from the owning aggregation, and is used as an upper bound on the
     *                    owning bucket ordinals.
     */
    public static LongKeyedBucketOrds build(BigArrays bigArrays, CardinalityUpperBound cardinality) {
        return cardinality.map(estimate -> estimate < 2 ? new FromSingle(bigArrays) : new FromMany(bigArrays));
    }

    /**
     * Build a {@link LongKeyedBucketOrds} who's values have known bounds.
     *
     * @param cardinality - This should come from the owning aggregation, and is used as an upper bound on the
     *                    owning bucket ordinals.
     * @param  min - The minimum key value for this aggregation
     * @param max - The maximum key value for this aggregation
     */
    public static LongKeyedBucketOrds buildForValueRange(BigArrays bigArrays, CardinalityUpperBound cardinality, long min, long max) {
        return cardinality.map((int cardinalityUpperBound) -> {
            if (cardinalityUpperBound < 2) {
                return new FromSingle(bigArrays);
            }
            if (min < 0 || cardinalityUpperBound == Integer.MAX_VALUE) {
                // cardinalityUpperBound tops out at maxint. If you see maxInt it could be anything above maxint.
                return new FromMany(bigArrays);
            }
            int owningBucketOrdShift = Long.numberOfLeadingZeros(cardinalityUpperBound);
            int maxBits = 64 - Long.numberOfLeadingZeros(max);
            if (maxBits < owningBucketOrdShift) {
                // There is enough space in a long to contain both the owning bucket and the entire range of values
                return new FromManySmall(bigArrays, owningBucketOrdShift);
            }
            return new FromMany(bigArrays);
        });
    }

    private TreeSet<Long> keySet = null;

    private LongKeyedBucketOrds() {}

    /**
     * Add the {@code owningBucketOrd, value} pair. Return the ord for
     * their bucket if they have yet to be added, or {@code -1-ord}
     * if they were already present.
     */
    public abstract long add(long owningBucketOrd, long value);

    /**
     * Count the buckets in {@code owningBucketOrd}.
     * <p>
     * Some aggregations expect this to be fast but most wouldn't
     * mind particularly if it weren't.
     */
    public abstract long bucketsInOrd(long owningBucketOrd);

    /**
     * Find the {@code owningBucketOrd, value} pair. Return the ord for
     * their bucket if they have been added or {@code -1} if they haven't.
     */
    public abstract long find(long owningBucketOrd, long value);

    /**
     * Returns the value currently associated with the bucket ordinal.
     */
    public abstract long get(long ordinal);

    /**
     * The number of collected buckets.
     */
    public abstract long size();

    /**
     * The maximum possible used {@code owningBucketOrd}.
     */
    public abstract long maxOwningBucketOrd();

    /**
     * Description used in profile results.
     */
    public abstract String decribe();

    /**
     * Build an iterator for buckets inside {@code owningBucketOrd} in order
     * of increasing ord.
     * <p>
     * When this is first returns it is "unpositioned" and you must call
     * {@link BucketOrdsEnum#next()} to move it to the first value.
     */
    public abstract BucketOrdsEnum ordsEnum(long owningBucketOrd);

    /**
     * Return an iterator for all keys in the given owning bucket, ordered in natural sort order.
     * This is suitable for aligning buckets across different instances of an aggregation.
     *
     * @param owningBucketOrd Only return keys that occurred under this owning bucket
     * @return a sorted iterator of long key values
     */
    public Iterator<Long> keyOrderedIterator(long owningBucketOrd) {
        if (keySet == null) {
            // TreeSet's contract includes a naturally ordered iterator
            keySet = new TreeSet<>();
            for (long ord = 0; ord < size(); ord++) {
                keySet.add(this.get(ord));
            }
        }
        Iterator<Long> toReturn = new Iterator<>() {
            final Iterator<Long> wrapped = keySet.iterator();
            final long filterOrd = owningBucketOrd;
            long next;
            boolean hasNext = true;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Long next() {
                if (hasNext == false) {
                    throw new NoSuchElementException();
                }
                long toReturn = next;
                hasNext = false;
                while (wrapped.hasNext()) {
                    long candidate = wrapped.next();
                    if (find(filterOrd, candidate) != -1) {
                        next = candidate;
                        hasNext = true;
                        break;
                    }
                }
                return toReturn;
            }
        };
        toReturn.next(); // Prime the first actual value
        return toReturn;
    }

    public void close() {
        keySet = null;
    }

    /**
     * An iterator for buckets inside a particular {@code owningBucketOrd}.
     */
    public interface BucketOrdsEnum {
        /**
         * Advance to the next value.
         * @return {@code true} if there *is* a next value,
         *         {@code false} if there isn't
         */
        boolean next();

        /**
         * The ordinal of the current value.
         */
        long ord();

        /**
         * The current value.
         */
        long value();

        /**
         * An {@linkplain BucketOrdsEnum} that is empty.
         */
        BucketOrdsEnum EMPTY = new BucketOrdsEnum() {
            @Override
            public boolean next() {
                return false;
            }

            @Override
            public long ord() {
                return 0;
            }

            @Override
            public long value() {
                return 0;
            }
        };
    }

    /**
     * Implementation that only works if it is collecting from a single bucket.
     */
    public static class FromSingle extends LongKeyedBucketOrds {
        private final LongHash ords;

        public FromSingle(BigArrays bigArrays) {
            ords = new LongHash(1, bigArrays);
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            // This is in the critical path for collecting most aggs. Be careful of performance.
            assert owningBucketOrd == 0;
            return ords.add(value);
        }

        @Override
        public long find(long owningBucketOrd, long value) {
            assert owningBucketOrd == 0;
            return ords.find(value);
        }

        @Override
        public long get(long ordinal) {
            return ords.get(ordinal);
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            assert owningBucketOrd == 0;
            return ords.size();
        }

        @Override
        public long size() {
            return ords.size();
        }

        @Override
        public long maxOwningBucketOrd() {
            return 0;
        }

        @Override
        public String decribe() {
            return "single bucket ords";
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            assert owningBucketOrd == 0;
            return new BucketOrdsEnum() {
                private long ord = -1;
                private long value;

                @Override
                public boolean next() {
                    ord++;
                    if (ord >= ords.size()) {
                        return false;
                    }
                    value = ords.get(ord);
                    return true;
                }

                @Override
                public long value() {
                    return value;
                }

                @Override
                public long ord() {
                    return ord;
                }
            };
        }

        @Override
        public void close() {
            super.close();
            ords.close();
        }
    }

    /**
     * Implementation that works properly when collecting from many buckets.
     */
    public static class FromMany extends LongKeyedBucketOrds {
        private final LongLongHash ords;
        private final OwnerChains chains;

        public FromMany(BigArrays bigArrays) {
            ords = new LongLongHash(2, bigArrays);
            boolean success = false;
            try {
                chains = new OwnerChains(bigArrays);
                success = true;
            } finally {
                if (success == false) {
                    ords.close();
                }
            }
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            // This is in the critical path for collecting most aggs. Be careful of performance.
            chains.ensureOwner(owningBucketOrd);
            long ord = ords.add(owningBucketOrd, value);
            if (ord >= 0) {
                chains.append(owningBucketOrd, ord);
            }
            return ord;
        }

        @Override
        public long find(long owningBucketOrd, long value) {
            return ords.find(owningBucketOrd, value);
        }

        @Override
        public long get(long ordinal) {
            return ords.getKey2(ordinal);
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            return chains.count(owningBucketOrd);
        }

        @Override
        public long size() {
            return ords.size();
        }

        @Override
        public long maxOwningBucketOrd() {
            return chains.maxOwningBucketOrd();
        }

        @Override
        public String decribe() {
            return "many bucket ords";
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            return new ChainedOrdsEnum(chains, owningBucketOrd) {
                @Override
                protected long valueOf(long ord) {
                    return ords.getKey2(ord);
                }
            };
        }

        @Override
        public void close() {
            super.close();
            Releasables.close(ords, chains);
        }
    }

    /**
     * Implementation that packs the {@code owningbucketOrd} into the top
     * bits of a {@code long} and uses the bottom bits for the value.
     */
    public static class FromManySmall extends LongKeyedBucketOrds {
        private final LongHash ords;
        private final OwnerChains chains;
        private final int owningBucketOrdShift;
        private final long owningBucketOrdMask;

        public FromManySmall(BigArrays bigArrays, int owningBucketOrdShift) {
            ords = new LongHash(2, bigArrays);
            boolean success = false;
            try {
                chains = new OwnerChains(bigArrays);
                success = true;
            } finally {
                if (success == false) {
                    ords.close();
                }
            }
            this.owningBucketOrdShift = owningBucketOrdShift;
            this.owningBucketOrdMask = -1L << owningBucketOrdShift;
        }

        private long encode(long owningBucketOrd, long value) {
            // This is in the critical path for collecting some aggs. Be careful of performance.
            return (owningBucketOrd << owningBucketOrdShift) | value;
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            // This is in the critical path for collecting lots of aggs. Be careful of performance.
            long enc = encode(owningBucketOrd, value);
            if (owningBucketOrd != (enc >>> owningBucketOrdShift) && (enc & ~owningBucketOrdMask) != value) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "[%s] and [%s] must fit in [%s..%s] bits",
                        owningBucketOrd,
                        value,
                        64 - owningBucketOrdShift,
                        owningBucketOrdShift
                    )
                );
            }
            chains.ensureOwner(owningBucketOrd);
            long ord = ords.add(enc);
            if (ord >= 0) {
                chains.append(owningBucketOrd, ord);
            }
            return ord;
        }

        @Override
        public long find(long owningBucketOrd, long value) {
            if (Long.numberOfLeadingZeros(owningBucketOrd) < owningBucketOrdShift) {
                return -1;
            }
            if ((value & owningBucketOrdMask) != 0) {
                return -1;
            }
            return ords.find(encode(owningBucketOrd, value));
        }

        @Override
        public long get(long ordinal) {
            return ords.get(ordinal) & ~owningBucketOrdMask;
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            return chains.count(owningBucketOrd);
        }

        @Override
        public long size() {
            return ords.size();
        }

        @Override
        public long maxOwningBucketOrd() {
            return chains.maxOwningBucketOrd();
        }

        @Override
        public String decribe() {
            return "many bucket ords packed using [" + (64 - owningBucketOrdShift) + "/" + owningBucketOrdShift + "] bits";
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            if (Long.numberOfLeadingZeros(owningBucketOrd) < owningBucketOrdShift) {
                return BucketOrdsEnum.EMPTY;
            }
            return new ChainedOrdsEnum(chains, owningBucketOrd) {
                @Override
                protected long valueOf(long ord) {
                    return ords.get(ord) & ~owningBucketOrdMask;
                }
            };
        }

        @Override
        public void close() {
            super.close();
            Releasables.close(ords, chains);
        }
    }

    /**
     * Links the bucket ordinals of each owning bucket into a list, in the
     * order they were added, so that counting and iterating the buckets of
     * one owning bucket costs time proportional to that owning bucket's
     * buckets rather than to all collected buckets. Building results visits
     * every owning bucket, so a full scan per owning bucket would cost
     * {@code owning buckets * total buckets}.
     * <p>
     * Uses one {@code long} per bucket ordinal plus three per owning bucket
     * ordinal up to the highest one added. Ordinals are stored plus one so
     * the zeroes of freshly allocated pages mean "none".
     */
    static final class OwnerChains implements Releasable {
        private final BigArrays bigArrays;
        /**
         * For each bucket ordinal, the next bucket ordinal of the same owning bucket, plus one.
         */
        private LongArray next;
        /**
         * For each owning bucket ordinal, its first bucket ordinal, plus one.
         */
        private LongArray heads;
        /**
         * For each owning bucket ordinal, its last bucket ordinal, plus one.
         */
        private LongArray tails;
        /**
         * For each owning bucket ordinal, its number of buckets.
         */
        private LongArray counts;
        private long maxOwningBucketOrd = -1;

        OwnerChains(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            boolean success = false;
            try {
                next = bigArrays.newLongArray(1, true);
                heads = bigArrays.newLongArray(1, true);
                tails = bigArrays.newLongArray(1, true);
                counts = bigArrays.newLongArray(1, true);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        /**
         * Make room for {@code owningBucketOrd}. Called before the bucket is
         * added to the hash so that a failure to allocate leaves no bucket
         * ordinal without a place in the lists.
         */
        void ensureOwner(long owningBucketOrd) {
            assert owningBucketOrd >= 0 : "owning bucket ordinals are never negative but got [" + owningBucketOrd + "]";
            if (owningBucketOrd >= heads.size()) {
                heads = bigArrays.grow(heads, owningBucketOrd + 1);
                tails = bigArrays.grow(tails, owningBucketOrd + 1);
                counts = bigArrays.grow(counts, owningBucketOrd + 1);
            }
        }

        /**
         * Append a newly added bucket ordinal to the end of its owning bucket's list.
         */
        void append(long owningBucketOrd, long ord) {
            next = bigArrays.grow(next, ord + 1);
            long tail = tails.get(owningBucketOrd);
            if (tail == 0) {
                heads.set(owningBucketOrd, ord + 1);
            } else {
                next.set(tail - 1, ord + 1);
            }
            tails.set(owningBucketOrd, ord + 1);
            counts.increment(owningBucketOrd, 1);
            maxOwningBucketOrd = Math.max(maxOwningBucketOrd, owningBucketOrd);
        }

        long count(long owningBucketOrd) {
            if (owningBucketOrd < 0 || owningBucketOrd > maxOwningBucketOrd) {
                return 0;
            }
            return counts.get(owningBucketOrd);
        }

        long maxOwningBucketOrd() {
            return maxOwningBucketOrd;
        }

        /**
         * The first bucket ordinal of {@code owningBucketOrd}, or {@code -1} if it has none.
         */
        long first(long owningBucketOrd) {
            if (owningBucketOrd < 0 || owningBucketOrd > maxOwningBucketOrd) {
                return -1;
            }
            return heads.get(owningBucketOrd) - 1;
        }

        /**
         * The bucket ordinal after {@code ord} in the same owning bucket, or {@code -1} if there is none.
         */
        long next(long ord) {
            return next.get(ord) - 1;
        }

        @Override
        public void close() {
            Releasables.close(next, heads, tails, counts);
        }
    }

    /**
     * Walks one owning bucket's list in {@link OwnerChains}. Reads each link
     * when advancing, so buckets appended to the owning bucket while
     * iterating are visited.
     */
    private abstract static class ChainedOrdsEnum implements BucketOrdsEnum {
        private final OwnerChains chains;
        private final long owningBucketOrd;
        private long ord = -1;
        private long value;

        ChainedOrdsEnum(OwnerChains chains, long owningBucketOrd) {
            this.chains = chains;
            this.owningBucketOrd = owningBucketOrd;
        }

        protected abstract long valueOf(long ord);

        @Override
        public boolean next() {
            long nextOrd = ord < 0 ? chains.first(owningBucketOrd) : chains.next(ord);
            if (nextOrd < 0) {
                return false;
            }
            ord = nextOrd;
            value = valueOf(ord);
            return true;
        }

        @Override
        public long value() {
            return value;
        }

        @Override
        public long ord() {
            return ord;
        }
    }
}

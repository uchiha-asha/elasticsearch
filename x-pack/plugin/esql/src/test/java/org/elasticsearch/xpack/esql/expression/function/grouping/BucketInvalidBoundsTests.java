/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.Duration;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Pins the reasons {@link Bucket#invalidBounds} gives for each kind of invalid {@code from}..{@code to} range, and the
 * ranges it leaves to the rest of {@code BUCKET}: valid ones, absent ones and ones that do not fold yet.
 */
public class BucketInvalidBoundsTests extends ESTestCase {

    private static Bucket numeric(Expression from, Expression to) {
        return new Bucket(Source.EMPTY, getFieldAttribute("salary", DataType.INTEGER), integer(10), from, to, null, TEST_CFG);
    }

    private static Bucket date(Expression buckets, Expression from, Expression to) {
        return new Bucket(Source.EMPTY, getFieldAttribute("hire_date", DataType.DATETIME), buckets, from, to, null, TEST_CFG);
    }

    private static String invalidBounds(Bucket bucket) {
        return bucket.invalidBounds(FoldContext.small());
    }

    private static Literal integer(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static Literal dbl(double value) {
        return new Literal(Source.EMPTY, value, DataType.DOUBLE);
    }

    private static Literal datetime(String value) {
        return new Literal(Source.EMPTY, dateTimeToLong(value), DataType.DATETIME);
    }

    private static Literal nullOf(DataType type) {
        return new Literal(Source.EMPTY, null, type);
    }

    public void testValidNumericRange() {
        assertThat(invalidBounds(numeric(integer(0), integer(100))), nullValue());
        assertThat(invalidBounds(numeric(dbl(-1.5), integer(0))), nullValue());
    }

    public void testNumericToBeforeFrom() {
        assertThat(invalidBounds(numeric(integer(50), integer(10))), equalTo("[to] [10] must be greater than [from] [50]"));
    }

    public void testNumericEmptyRange() {
        assertThat(invalidBounds(numeric(dbl(5.0), dbl(5.0))), equalTo("[to] [5.0] must be greater than [from] [5.0]"));
    }

    public void testNumericNonFiniteBound() {
        assertThat(invalidBounds(numeric(dbl(Double.NaN), integer(10))), equalTo("[from] [NaN] is not a finite number"));
        assertThat(invalidBounds(numeric(integer(0), dbl(Double.POSITIVE_INFINITY))), equalTo("[to] [Infinity] is not a finite number"));
    }

    public void testNullBound() {
        assertThat(invalidBounds(numeric(nullOf(DataType.NULL), integer(10))), equalTo("[from] is null"));
        assertThat(invalidBounds(numeric(integer(0), nullOf(DataType.INTEGER))), equalTo("[to] is null"));
        assertThat(invalidBounds(date(integer(10), datetime("1985-01-01T00:00:00Z"), nullOf(DataType.DATETIME))), equalTo("[to] is null"));
    }

    public void testValidDateRange() {
        assertThat(invalidBounds(date(integer(10), datetime("1985-01-01T00:00:00Z"), datetime("1986-01-01T00:00:00Z"))), nullValue());
        assertThat(
            invalidBounds(
                date(integer(10), Literal.keyword(Source.EMPTY, "1985-01-01T00:00:00Z"), Literal.keyword(Source.EMPTY, "1986-01-01"))
            ),
            nullValue()
        );
    }

    /** A date range with no width still picks a rounding, so it is not reported. */
    public void testDateEmptyRangeIsValid() {
        assertThat(invalidBounds(date(integer(10), datetime("1985-01-01T00:00:00Z"), datetime("1985-01-01T00:00:00Z"))), nullValue());
    }

    public void testDateToBeforeFrom() {
        assertThat(
            invalidBounds(
                date(integer(10), Literal.keyword(Source.EMPTY, "1986-01-01T00:00:00Z"), Literal.keyword(Source.EMPTY, "1985-01-01"))
            ),
            equalTo("[to] [1985-01-01T00:00:00.000Z] must not be before [from] [1986-01-01T00:00:00.000Z]")
        );
    }

    /** A fixed span with a range uses the range only to insert empty buckets, but the range is still reported. */
    public void testDateSpanToBeforeFrom() {
        Literal span = new Literal(Source.EMPTY, Duration.ofHours(1), DataType.TIME_DURATION);
        assertThat(
            invalidBounds(date(span, datetime("1986-01-01T00:00:00Z"), datetime("1985-01-01T00:00:00Z"))),
            equalTo("[to] [1985-01-01T00:00:00.000Z] must not be before [from] [1986-01-01T00:00:00.000Z]")
        );
    }

    /** An unparseable date is reported where the rounding is built, like for any other range. */
    public void testUnparseableDateIsLeftAlone() {
        assertThat(
            invalidBounds(date(integer(10), Literal.keyword(Source.EMPTY, "not a date"), datetime("1985-01-01T00:00:00Z"))),
            nullValue()
        );
    }

    public void testNoRange() {
        Bucket span = new Bucket(Source.EMPTY, getFieldAttribute("salary", DataType.INTEGER), dbl(100.0), null, null, null, TEST_CFG);
        assertThat(invalidBounds(span), nullValue());
    }

    /** A bound that does not fold is reported by the post-optimization verification instead. */
    public void testNonFoldableBound() {
        assertThat(invalidBounds(numeric(referenceAttribute("f", DataType.INTEGER), integer(10))), nullValue());
        assertThat(invalidBounds(numeric(integer(50), referenceAttribute("t", DataType.INTEGER))), nullValue());
    }
}

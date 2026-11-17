/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BitmapTermsQueryBuilderTests extends AbstractQueryTestCase<BitmapTermsQueryBuilder> {

    /** An {@code integer} field indexed with terms, which {@code bitmap_terms} queries with {@link BitmapTermsQuery}. */
    private static final String INT_TERMS_FIELD_NAME = "mapped_int_terms";
    private static final String INT_TERMS_ALIAS_FIELD_NAME = "mapped_int_terms_alias";
    /** A {@code long} field indexed with points, which {@code bitmap_terms} queries with {@link BitmapBKDQuery}. */
    private static final String LONG_FIELD_NAME = "mapped_bitmap_long";
    private static final String LONG_ALIAS_FIELD_NAME = "mapped_bitmap_long_alias";
    /** A {@code long} field indexed with terms, which {@code bitmap_terms} queries with {@link BitmapTermsQuery}. */
    private static final String LONG_TERMS_FIELD_NAME = "mapped_long_terms";
    private static final String LONG_TERMS_ALIAS_FIELD_NAME = "mapped_long_terms_alias";

    private static final List<String> INT_FIELDS = List.of(
        INT_FIELD_NAME,
        INT_ALIAS_FIELD_NAME,
        INT_TERMS_FIELD_NAME,
        INT_TERMS_ALIAS_FIELD_NAME
    );
    private static final List<String> LONG_FIELDS = List.of(
        LONG_FIELD_NAME,
        LONG_ALIAS_FIELD_NAME,
        LONG_TERMS_FIELD_NAME,
        LONG_TERMS_ALIAS_FIELD_NAME
    );

    private static final Map<String, String> ALIAS_TARGETS = Map.of(
        INT_TERMS_ALIAS_FIELD_NAME,
        INT_TERMS_FIELD_NAME,
        LONG_ALIAS_FIELD_NAME,
        LONG_FIELD_NAME,
        LONG_TERMS_ALIAS_FIELD_NAME,
        LONG_TERMS_FIELD_NAME
    );

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(BitmapPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(
                Strings.format(
                    """
                        {
                          "properties": {
                            "%s": { "type": "integer", "index_terms": true },
                            "%s": { "type": "alias", "path": "%s" },
                            "%s": { "type": "long" },
                            "%s": { "type": "alias", "path": "%s" },
                            "%s": { "type": "long", "index_terms": true },
                            "%s": { "type": "alias", "path": "%s" }
                          }
                        }""",
                    INT_TERMS_FIELD_NAME,
                    INT_TERMS_ALIAS_FIELD_NAME,
                    INT_TERMS_FIELD_NAME,
                    LONG_FIELD_NAME,
                    LONG_ALIAS_FIELD_NAME,
                    LONG_FIELD_NAME,
                    LONG_TERMS_FIELD_NAME,
                    LONG_TERMS_ALIAS_FIELD_NAME,
                    LONG_TERMS_FIELD_NAME
                )
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    /** Resolves the aliases this test maps, and those {@link AbstractQueryTestCase} maps, to their target fields. */
    private static String targetFieldName(String builderFieldName) {
        String target = ALIAS_TARGETS.get(builderFieldName);
        return target != null ? target : expectedFieldName(builderFieldName);
    }

    @Override
    protected BitmapTermsQueryBuilder doCreateTestQueryBuilder() {
        if (randomBoolean()) {
            return new BitmapTermsQueryBuilder(randomFrom(INT_FIELDS), randomIntBitmap());
        }
        return new BitmapTermsQueryBuilder(randomFrom(LONG_FIELDS), randomLongBitmap());
    }

    @Override
    protected void doAssertLuceneQuery(BitmapTermsQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        String expectedField = targetFieldName(queryBuilder.fieldName());
        boolean indexedWithTerms = expectedField.equals(INT_TERMS_FIELD_NAME) || expectedField.equals(LONG_TERMS_FIELD_NAME);
        if (indexedWithTerms) {
            assertThat(query, instanceOf(BitmapTermsQuery.class));
        } else {
            assertThat(query, instanceOf(BitmapBKDQuery.class));
        }
        assertThat(query, equalTo(expectedQuery(expectedField, indexedWithTerms, queryBuilder.value())));
    }

    public void testUnmappedFieldRewritesToMatchNone() throws IOException {
        BitmapTermsQueryBuilder builder = new BitmapTermsQueryBuilder("unmapped_field", randomIntBitmap());
        QueryBuilder rewritten = rewriteQuery(builder, createQueryRewriteContext(), createSearchExecutionContext());
        assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
        assertThat(builder.toQuery(createSearchExecutionContext()), instanceOf(MatchNoDocsQuery.class));
    }

    public void testMappedFieldIsNotRewritten() throws IOException {
        BitmapTermsQueryBuilder builder = new BitmapTermsQueryBuilder(randomFrom(INT_FIELDS), randomIntBitmap());
        QueryBuilder rewritten = rewriteQuery(builder, createQueryRewriteContext(), createSearchExecutionContext());
        assertThat(rewritten, equalTo(builder));
    }

    public void testAliasQueriesTargetField() throws IOException {
        String intValue = randomIntBitmap();
        assertThat(
            new BitmapTermsQueryBuilder(INT_ALIAS_FIELD_NAME, intValue).toQuery(createSearchExecutionContext()),
            equalTo(expectedQuery(INT_FIELD_NAME, false, intValue))
        );
        assertThat(
            new BitmapTermsQueryBuilder(INT_TERMS_ALIAS_FIELD_NAME, intValue).toQuery(createSearchExecutionContext()),
            equalTo(expectedQuery(INT_TERMS_FIELD_NAME, true, intValue))
        );
        String longValue = randomLongBitmap();
        assertThat(
            new BitmapTermsQueryBuilder(LONG_ALIAS_FIELD_NAME, longValue).toQuery(createSearchExecutionContext()),
            equalTo(expectedQuery(LONG_FIELD_NAME, false, longValue))
        );
        assertThat(
            new BitmapTermsQueryBuilder(LONG_TERMS_ALIAS_FIELD_NAME, longValue).toQuery(createSearchExecutionContext()),
            equalTo(expectedQuery(LONG_TERMS_FIELD_NAME, true, longValue))
        );
    }

    public void testUnsupportedMappedFieldIsRejected() {
        String field = randomFrom(TEXT_FIELD_NAME, KEYWORD_FIELD_NAME, DOUBLE_FIELD_NAME);
        BitmapTermsQueryBuilder builder = new BitmapTermsQueryBuilder(field, randomIntBitmap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("[bitmap_terms] query is not supported on field [" + field + "]"));
    }

    private static Query expectedQuery(String field, boolean indexedWithTerms, String value) {
        byte[] bytes = Base64.getDecoder().decode(value);
        BitmapValues values;
        try {
            values = field.equals(LONG_FIELD_NAME) || field.equals(LONG_TERMS_FIELD_NAME)
                ? LongBitmap.deserializePortable(bytes)
                : IntBitmap.deserialize(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return indexedWithTerms ? new BitmapTermsQuery(field, values) : new BitmapBKDQuery(field, values);
    }

    private static String randomIntBitmap() {
        RoaringBitmap bitmap = new RoaringBitmap();
        int count = between(0, 20);
        for (int i = 0; i < count; i++) {
            bitmap.add(randomIntBetween(0, Integer.MAX_VALUE));
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            bitmap.serialize(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }

    private static String randomLongBitmap() {
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        int count = between(0, 20);
        for (int i = 0; i < count; i++) {
            bitmap.addLong(randomLongBetween(0, Long.MAX_VALUE));
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            bitmap.serializePortable(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }
}

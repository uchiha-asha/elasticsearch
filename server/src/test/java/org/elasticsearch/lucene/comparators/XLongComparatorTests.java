/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.comparators;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class XLongComparatorTests extends ESTestCase {

    private static final String FIELD = "@timestamp";

    /**
     * A leaf comparator opens the sort field's doc values on its first read of a sort value, not when it is built.
     */
    public void testDocValuesOpenedOnFirstRead() throws IOException {
        try (Directory dir = newDirectory()) {
            // one flush, so doc ids follow insertion order
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new LongPoint(FIELD, i));
                    doc.add(new SortedNumericDocValuesField(FIELD, i));
                    writer.addDocument(doc);
                }
            }
            AtomicInteger opens = new AtomicInteger();
            try (DirectoryReader reader = new CountingDirectoryReader(DirectoryReader.open(dir), opens)) {
                LeafReaderContext leaf = reader.leaves().getFirst();
                XLongComparator comparator = (XLongComparator) source(randomFrom(MultiValueMode.MIN, MultiValueMode.MAX)).newComparator(
                    FIELD,
                    2,
                    randomFrom(Pruning.values()),
                    randomBoolean()
                );
                LeafFieldComparator leafComparator = comparator.getLeafComparator(leaf);
                leafComparator.setScorer(null);
                assertThat(opens.get(), equalTo(0));

                leafComparator.copy(0, 3);
                assertThat(opens.get(), equalTo(1));
                leafComparator.copy(1, 7);
                leafComparator.setBottom(0);
                leafComparator.compareBottom(8);
                assertThat(opens.get(), equalTo(1));
                assertThat(comparator.value(0), equalTo(3L));
                assertThat(comparator.value(1), equalTo(7L));
            }
        }
    }

    /**
     * Top hits and sort values, first page and {@code search_after} pages, match Lucene's own {@link SortedNumericSortField}
     * on a multi-segment index with missing and multi-valued fields and a query that matches few segments; doc values are
     * opened only on segments that hold a match.
     */
    public void testSortMatchesLuceneSortOnSparseMatches() throws IOException {
        try (Directory dir = newDirectory()) {
            int segments = between(2, 20);
            IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc)) {
                for (int s = 0; s < segments; s++) {
                    boolean matches = randomBoolean();
                    int docs = between(1, 200);
                    for (int d = 0; d < docs; d++) {
                        Document doc = new Document();
                        if (matches && randomBoolean()) {
                            doc.add(new StringField("tag", "match", Field.Store.NO));
                        }
                        int values = between(0, 3);
                        for (int v = 0; v < values; v++) {
                            long value = randomLongBetween(-1000, 1000);
                            doc.add(new LongPoint(FIELD, value));
                            doc.add(new SortedNumericDocValuesField(FIELD, value));
                        }
                        writer.addDocument(doc);
                    }
                    writer.flush();
                }
            }
            AtomicInteger opens = new AtomicInteger();
            try (DirectoryReader reader = new CountingDirectoryReader(DirectoryReader.open(dir), opens)) {
                IndexSearcher searcher = newSearcher(reader, false, false);
                Query query = new TermQuery(new Term("tag", "match"));
                int matchingLeaves = 0;
                Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
                for (LeafReaderContext leaf : reader.leaves()) {
                    if (weight.scorer(leaf) != null) {
                        matchingLeaves++;
                    }
                }
                MultiValueMode mode = randomFrom(MultiValueMode.MIN, MultiValueMode.MAX);
                boolean reverse = randomBoolean();
                String missing = randomFrom("_first", "_last");
                LongValuesComparatorSource source = new LongValuesComparatorSource(
                    fieldData(),
                    missing,
                    mode,
                    null,
                    IndexNumericFieldData.NumericType.LONG
                );
                SortField esSortField = new SortField(FIELD, source, reverse);
                SortedNumericSortField luceneSortField = new SortedNumericSortField(
                    FIELD,
                    SortField.Type.LONG,
                    reverse,
                    mode == MultiValueMode.MAX ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN
                );
                luceneSortField.setMissingValue(source.missingObject(missing, reverse));
                // tie-break on doc id so both sorts define one order
                Sort esSort = new Sort(esSortField, SortField.FIELD_DOC);
                Sort luceneSort = new Sort(luceneSortField, SortField.FIELD_DOC);

                int size = between(1, 10);
                FieldDoc esAfter = null;
                FieldDoc luceneAfter = null;
                int pages = 0;
                while (true) {
                    opens.set(0);
                    TopFieldDocs esHits = searcher.search(query, new TopFieldCollectorManager(esSort, size, esAfter, Integer.MAX_VALUE));
                    assertThat(opens.get(), lessThanOrEqualTo(matchingLeaves));
                    TopFieldDocs luceneHits = searcher.search(
                        query,
                        new TopFieldCollectorManager(luceneSort, size, luceneAfter, Integer.MAX_VALUE)
                    );
                    assertThat(esHits.totalHits, equalTo(luceneHits.totalHits));
                    assertThat(esHits.scoreDocs.length, equalTo(luceneHits.scoreDocs.length));
                    for (int i = 0; i < esHits.scoreDocs.length; i++) {
                        FieldDoc es = (FieldDoc) esHits.scoreDocs[i];
                        FieldDoc lucene = (FieldDoc) luceneHits.scoreDocs[i];
                        assertThat(es.doc, equalTo(lucene.doc));
                        assertThat(es.fields[0], equalTo(lucene.fields[0]));
                    }
                    if (esHits.scoreDocs.length < size || ++pages > 50) {
                        break;
                    }
                    ScoreDoc[] docs = esHits.scoreDocs;
                    esAfter = (FieldDoc) docs[docs.length - 1];
                    luceneAfter = (FieldDoc) luceneHits.scoreDocs[docs.length - 1];
                }
            }
        }
    }

    private static SortedNumericIndexFieldData fieldData() {
        return new SortedNumericIndexFieldData(
            FIELD,
            IndexNumericFieldData.NumericType.LONG,
            CoreValuesSourceType.NUMERIC,
            null,
            IndexType.points(true, true)
        );
    }

    private static LongValuesComparatorSource source(MultiValueMode mode) {
        return new LongValuesComparatorSource(fieldData(), null, mode, null, IndexNumericFieldData.NumericType.LONG);
    }

    /**
     * Counts how often the sort field's doc values are opened. The cache helpers of the wrapped readers are passed through
     * so the wrapper does not change caching behaviour.
     */
    private static class CountingDirectoryReader extends FilterDirectoryReader {
        private final AtomicInteger opens;

        CountingDirectoryReader(DirectoryReader in, AtomicInteger opens) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FilterLeafReader(reader) {
                        @Override
                        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
                            if (field.equals(FIELD)) {
                                opens.incrementAndGet();
                            }
                            return super.getSortedNumericDocValues(field);
                        }

                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return in.getCoreCacheHelper();
                        }

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return in.getReaderCacheHelper();
                        }
                    };
                }
            });
            this.opens = opens;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new CountingDirectoryReader(in, opens);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}

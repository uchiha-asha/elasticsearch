/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.fielddata.DenseLongValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LongValuesComparatorSourceTests extends ESTestCase {

    /** The id of the document that sorts in the middle of the segment built by {@link #indexMultiValued}. */
    private static final long MIDDLE_ID = 0;

    /**
     * The {@link NumericDocValues} produced by {@link LongValuesComparatorSource#wrap} is used as a competitive iterator
     * during sorting. When such a sort runs under a conjunction query, Lucene 10.5's {@code DenseConjunctionBulkScorer}
     * calls {@link DocIdSetIterator#cost()} on every conjunction clause while scoring a window. The wrapper used to throw
     * {@link UnsupportedOperationException} from {@code cost()}, which surfaced as a query failure after the Lucene 10.5
     * upgrade (and, downstream, as an ML data-frame-analytics inference failure since {@code TestDocsIterator} search-afters
     * the destination index sorted by {@code ml__incremental_id}). The wrapper iterates densely over {@code [0, maxDoc)},
     * so its cost must be {@code maxDoc}, matching the sibling {@link org.elasticsearch.index.fielddata.DenseDoubleValues}.
     */
    public void testWrapReportsDenseCost() throws Exception {
        long[] values = new long[] { 5, -3, 42, 0, 17 };
        int maxDoc = values.length;
        NumericDocValues docValues = LongValuesComparatorSource.wrap(denseLongValues(values), maxDoc);

        // Must not throw and must report the dense cost, exactly as Lucene 10.5's conjunction bulk scorer expects.
        assertEquals(maxDoc, docValues.cost());
    }

    public void testWrapIteratesDenselyOverAllDocs() throws Exception {
        long[] values = new long[] { 5, -3, 42, 0, 17 };
        int maxDoc = values.length;

        // Iterating with nextDoc() must visit every document in order and expose the right value for each.
        NumericDocValues docValues = LongValuesComparatorSource.wrap(denseLongValues(values), maxDoc);
        assertEquals(-1, docValues.docID());
        for (int doc = 0; doc < maxDoc; doc++) {
            assertEquals(doc, docValues.nextDoc());
            assertEquals(doc, docValues.docID());
            assertEquals(values[doc], docValues.longValue());
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.nextDoc());

        // advance() past the last document terminates the iterator.
        docValues = LongValuesComparatorSource.wrap(denseLongValues(values), maxDoc);
        assertEquals(2, docValues.advance(2));
        assertEquals(values[2], docValues.longValue());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.advance(maxDoc));
    }

    /**
     * Index sorted by {@code @timestamp} descending on the max value, query sorted ascending on the min value: the segment order is
     * not the reverse of the query order, so the top hits come from the whole segment.
     */
    public void testMatchTailMultiValuedIndexMaxQueryMin() throws IOException {
        int perGroup = between(5, 20);
        int numHits = between(1, 3);
        try (Directory dir = newDirectory(); DirectoryReader reader = indexMultiValued(dir, SortedNumericSelector.Type.MAX, perGroup)) {
            List<Long> ids = topIds(reader, MultiValueMode.MIN, numHits, true);
            assertThat(ids, equalTo(topIds(reader, MultiValueMode.MIN, numHits, false)));
            // The B documents with the lowest minimums come first, they have the highest maximums among the B documents
            assertThat(ids.getFirst(), equalTo(groupBId(perGroup, perGroup - 1)));
        }
    }

    /**
     * Index sorted by {@code @timestamp} descending on the min value, query sorted ascending on the max value: the segment order is
     * not the reverse of the query order, so the top hits come from the whole segment.
     */
    public void testMatchTailMultiValuedIndexMinQueryMax() throws IOException {
        int perGroup = between(5, 20);
        int numHits = between(1, 3);
        try (Directory dir = newDirectory(); DirectoryReader reader = indexMultiValued(dir, SortedNumericSelector.Type.MIN, perGroup)) {
            List<Long> ids = topIds(reader, MultiValueMode.MAX, numHits, true);
            assertThat(ids, equalTo(topIds(reader, MultiValueMode.MAX, numHits, false)));
            // The B documents with the lowest maximums come first, they have the highest minimums among the B documents
            assertThat(ids.getFirst(), equalTo(groupBId(perGroup, 0)));
        }
    }

    /**
     * SUM, AVG and MEDIAN compute a value the index sort never orders by, so they read the whole segment whatever the index sort's
     * selector is.
     */
    public void testMatchTailMultiValuedQueryAvg() throws IOException {
        int perGroup = between(5, 20);
        int numHits = between(1, 3);
        MultiValueMode mode = randomFrom(MultiValueMode.SUM, MultiValueMode.AVG, MultiValueMode.MEDIAN);
        SortedNumericSelector.Type selector = randomFrom(SortedNumericSelector.Type.MIN, SortedNumericSelector.Type.MAX);
        try (Directory dir = newDirectory(); DirectoryReader reader = indexMultiValued(dir, selector, perGroup)) {
            List<Long> ids = topIds(reader, mode, numHits, true);
            assertThat(ids, equalTo(topIds(reader, mode, numHits, false)));
            assertThat(ids.getFirst(), equalTo(MIDDLE_ID));
        }
    }

    /**
     * When the index sort's selector is the value the query sorts on, the segment order is the reverse of the query order: the top hits
     * are right and only the tail of the segment is competitive.
     */
    public void testMatchTailMultiValuedMatchingSelector() throws IOException {
        int perGroup = between(5, 20);
        int numHits = between(1, 3);
        boolean min = randomBoolean();
        SortedNumericSelector.Type selector = min ? SortedNumericSelector.Type.MIN : SortedNumericSelector.Type.MAX;
        MultiValueMode mode = min ? MultiValueMode.MIN : MultiValueMode.MAX;
        try (Directory dir = newDirectory(); DirectoryReader reader = indexMultiValued(dir, selector, perGroup)) {
            assertThat(topIds(reader, mode, numHits, true), equalTo(topIds(reader, mode, numHits, false)));
            assertThat(firstCompetitiveDoc(reader, mode, numHits), equalTo(reader.maxDoc() - numHits - 1));
        }
    }

    /**
     * With a single {@code @timestamp} value per document every sort mode picks the same value, so the tail is read even though the
     * index sort's selector (max) differs from the query's mode.
     */
    public void testMatchTailSingleValuedMismatchedSelector() throws IOException {
        int numDocs = between(10, 100);
        int numHits = between(1, 3);
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig().setIndexSort(
                new Sort(new SortedNumericSortField("@timestamp", SortField.Type.LONG, true, SortedNumericSelector.Type.MAX))
            );
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numDocs; i++) {
                    addDoc(writer, i, 1_000 + 10L * i);
                }
                writer.forceMerge(1);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    List<Long> ids = topIds(reader, MultiValueMode.MIN, numHits, true);
                    assertThat(ids, equalTo(topIds(reader, MultiValueMode.MIN, numHits, false)));
                    assertThat(ids, contains(expectedFirstIds(numHits)));
                    assertThat(firstCompetitiveDoc(reader, MultiValueMode.MIN, numHits), equalTo(numDocs - numHits - 1));
                }
            }
        }
    }

    private static Long[] expectedFirstIds(int numHits) {
        Long[] ids = new Long[numHits];
        for (int i = 0; i < numHits; i++) {
            ids[i] = (long) i;
        }
        return ids;
    }

    private static long groupAId(int i) {
        return 1 + i;
    }

    private static long groupBId(int perGroup, int i) {
        return 1 + perGroup + i;
    }

    /**
     * Indexes one segment sorted by {@code @timestamp} descending with the given selector. Every value is unique, and:
     * <ul>
     *     <li>document {@link #MIDDLE_ID} {@code [0, 0, 5_000_000]} sorts in the middle of the segment for both the min and max
     *     selectors and has the lowest sum, average and median;</li>
     *     <li>the A documents {@code [1_000_000 + i, 10_000_000 + i]} sort before it for both selectors;</li>
     *     <li>the B documents {@code [-1_000_000 - i, 4_000_000 + i, 4_000_000 + i]} sort after it for both selectors, and they hold
     *     the lowest minimums and the lowest maximums.</li>
     * </ul>
     */
    private static DirectoryReader indexMultiValued(Directory dir, SortedNumericSelector.Type selector, int perGroup) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig().setIndexSort(
            new Sort(new SortedNumericSortField("@timestamp", SortField.Type.LONG, true, selector))
        );
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            addDoc(writer, MIDDLE_ID, 0, 0, 5_000_000);
            for (int i = 0; i < perGroup; i++) {
                addDoc(writer, groupAId(i), 1_000_000 + i, 10_000_000 + i);
                addDoc(writer, groupBId(perGroup, i), -1_000_000 - i, 4_000_000 + i, 4_000_000 + i);
            }
            writer.forceMerge(1);
            return DirectoryReader.open(writer);
        }
    }

    private static void addDoc(IndexWriter writer, long id, long... timestamps) throws IOException {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", id));
        for (long timestamp : timestamps) {
            doc.add(new SortedNumericDocValuesField("@timestamp", timestamp));
        }
        writer.addDocument(doc);
    }

    private static LongValuesComparatorSource source(MultiValueMode mode, boolean matchTail) {
        SortedNumericIndexFieldData fd = new SortedNumericIndexFieldData(
            "@timestamp",
            IndexNumericFieldData.NumericType.LONG,
            CoreValuesSourceType.NUMERIC,
            null,
            IndexType.NONE
        );
        LongValuesComparatorSource source = new LongValuesComparatorSource(fd, null, mode, null, IndexNumericFieldData.NumericType.LONG);
        if (matchTail) {
            source.setMatchTailQuery();
        }
        return source;
    }

    /** The ids of the top hits of a match-all search sorted by {@code @timestamp} ascending with the given mode. */
    private static List<Long> topIds(DirectoryReader reader, MultiValueMode mode, int numHits, boolean matchTail) throws IOException {
        assertThat(reader.leaves(), hasSize(1));
        Sort sort = new Sort(new SortField("@timestamp", source(mode, matchTail), false));
        TopFieldDocs topDocs = new IndexSearcher(reader).search(
            MatchAllDocsQuery.INSTANCE,
            new TopFieldCollectorManager(sort, numHits, null, numHits)
        );
        List<Long> ids = new ArrayList<>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            // Hits are in sort order, not doc id order, so each one reads the ids from a new iterator
            NumericDocValues idValues = DocValues.getNumeric(reader.leaves().getFirst().reader(), "id");
            assertTrue(idValues.advanceExact(scoreDoc.doc));
            ids.add(idValues.longValue());
        }
        return ids;
    }

    /** The first doc of the competitive iterator after collecting enough docs to fill the queue and reach the hits threshold. */
    private static int firstCompetitiveDoc(DirectoryReader reader, MultiValueMode mode, int numHits) throws IOException {
        Sort sort = new Sort(new SortField("@timestamp", source(mode, true), false));
        LeafReaderContext leaf = reader.leaves().getFirst();
        LeafCollector leafCollector = new TopFieldCollectorManager(sort, numHits, null, numHits).newCollector().getLeafCollector(leaf);
        DocIdSetIterator iterator = leafCollector.competitiveIterator();
        for (int doc = 0; doc <= numHits; doc++) {
            leafCollector.collect(doc);
        }
        return iterator.nextDoc();
    }

    private static DenseLongValues denseLongValues(long[] values) {
        return new DenseLongValues() {
            private long current;

            @Override
            protected void doAdvanceExact(int doc) {
                current = values[doc];
            }

            @Override
            public long longValue() {
                return current;
            }
        };
    }
}
